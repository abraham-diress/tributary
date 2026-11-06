package com.tributary.client;

import com.tributary.common.BrokerInfo;
import com.tributary.common.Errors;
import com.tributary.common.KafkaException;
import com.tributary.common.OffsetOutOfRangeException;
import com.tributary.common.Protocol;
import com.tributary.storage.Record;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Low-level client. It discovers the cluster from any bootstrap broker, caches metadata (who leads
 * each partition), routes produce/fetch requests to the right leader, and transparently refreshes
 * metadata and retries when a leader moves or a broker goes down.
 *
 * <p>Retrying a produce after a network error can duplicate a record if the first attempt actually
 * succeeded, so delivery is at-least-once.
 */
public class TributaryClient implements Closeable {
    private final List<InetSocketAddress> bootstrapServers;
    private final Map<String, BrokerConnection> connections = new ConcurrentHashMap<>();
    private volatile Protocol.MetadataResult metadata = new Protocol.MetadataResult(-1, List.of(), List.of());
    private final int requestTimeoutMs;
    private final int maxRetries;
    private final long retryBackoffMs;

    @FunctionalInterface
    private interface Request<T> {
        T run() throws IOException;
    }

    public TributaryClient(String bootstrapBroker, int bootstrapPort) {
        this(bootstrapBroker + ":" + bootstrapPort);
    }

    /** @param bootstrapServers comma-separated host:port list */
    public TributaryClient(String bootstrapServers) {
        this(bootstrapServers, 30_000, 8, 250);
    }

    public TributaryClient(String bootstrapServers, int requestTimeoutMs, int maxRetries, long retryBackoffMs) {
        this.bootstrapServers = parseServers(bootstrapServers);
        this.requestTimeoutMs = requestTimeoutMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    // ------------------------------------------------------------------ metadata

    /** Fetches cluster metadata from the first broker that answers. */
    public synchronized void refreshMetadata() throws IOException {
        Set<InetSocketAddress> candidates = new LinkedHashSet<>();
        for (BrokerInfo b : metadata.brokers()) {
            candidates.add(InetSocketAddress.createUnresolved(b.getHost(), b.getPort()));
        }
        candidates.addAll(bootstrapServers);
        IOException last = null;
        for (InetSocketAddress address : candidates) {
            try {
                ByteBuffer response = connection(address.getHostString(), address.getPort()).send(Protocol.encodeMetadataRequest());
                metadata = Protocol.decodeMetadataResponse(response);
                return;
            } catch (IOException e) {
                last = e;
            }
        }
        throw new IOException("Unable to fetch metadata from any broker " + candidates, last);
    }

    public Protocol.MetadataResult metadata() {
        return metadata;
    }

    public Protocol.TopicMetadata topicMetadata(String topic) throws IOException {
        Protocol.TopicMetadata t = metadata.topic(topic);
        if (t == null) {
            refreshMetadata();
            t = metadata.topic(topic);
        }
        return t;
    }

    public boolean topicExists(String topic) throws IOException {
        return topicMetadata(topic) != null;
    }

    public int partitionCount(String topic) throws IOException {
        Protocol.TopicMetadata t = topicMetadata(topic);
        if (t == null) {
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "Topic '" + topic + "' does not exist");
        }
        return t.partitions().size();
    }

    // ------------------------------------------------------------------ admin

    /** Asks the controller to create a topic, then waits until every partition has a leader. */
    public void createTopic(String topic, int numPartitions, int replicationFactor) throws IOException {
        withRetries(() -> {
            BrokerInfo controller = metadata.broker(metadata.controllerId());
            if (controller == null) {
                throw new KafkaException(Errors.NOT_CONTROLLER, "Controller not known yet");
            }
            ByteBuffer response = connection(controller).send(
                    Protocol.encodeCreateTopicRequest(topic, numPartitions, (short) replicationFactor));
            Protocol.checkResponse(response, Protocol.CREATE_TOPIC);
            return null;
        });
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            refreshMetadata();
            Protocol.TopicMetadata t = metadata.topic(topic);
            if (t != null && t.partitions().size() == numPartitions
                    && t.partitions().stream().allMatch(p -> p.leader() >= 0)) {
                return;
            }
            sleep(100);
        }
        throw new IOException("Topic '" + topic + "' was created but its partitions did not come online in time");
    }

    // ------------------------------------------------------------------ produce

    /** Sends a single message to a specific partition and returns its offset. */
    public long send(String topic, int partition, byte[] message) throws IOException {
        return produce(topic, partition, List.of(Record.of(null, message)), Protocol.ACKS_LEADER);
    }

    /** Sends a batch to a partition's leader. Returns the offset assigned to the first record. */
    public long produce(String topic, int partition, List<Record> records, short acks) throws IOException {
        ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, acks, requestTimeoutMs / 2, records);
        return withRetries(() -> Protocol.decodeProduceResponse(leaderConnection(topic, partition).send(request)).baseOffset());
    }

    // ------------------------------------------------------------------ fetch

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        List<byte[]> messages = new ArrayList<>();
        for (Record r : fetch(topic, partition, offset, maxBytes, 0).records()) {
            messages.add(r.value());
        }
        return messages;
    }

    /**
     * Fetches committed records starting at {@code offset}. If none are available yet, the leader
     * holds the request for up to {@code maxWaitMs} (long polling) instead of returning empty.
     */
    public Protocol.FetchResult fetch(String topic, int partition, long offset, int maxBytes, int maxWaitMs) throws IOException {
        ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes, maxWaitMs,
                Protocol.CONSUMER_REPLICA_ID, -1);
        return withRetries(() -> Protocol.decodeFetchResponse(leaderConnection(topic, partition).send(request)));
    }

    /** Returns {logStartOffset, logEndOffset} for a partition. */
    public long[] offsetRange(String topic, int partition) throws IOException {
        try {
            fetch(topic, partition, -1, 1, 0);
        } catch (OffsetOutOfRangeException e) {
            return new long[]{e.logStartOffset(), e.logEndOffset()};
        }
        throw new IOException("Unexpected response to offset range probe");
    }

    // ------------------------------------------------------------------ consumer offsets

    public void commitOffset(String group, String topic, int partition, long offset) throws IOException {
        ByteBuffer request = Protocol.encodeOffsetCommitRequest(group, topic, partition, offset);
        withRetries(() -> {
            Protocol.checkResponse(anyConnection().send(request), Protocol.OFFSET_COMMIT);
            return null;
        });
    }

    /** Returns the committed offset, or -1 if the group has never committed for this partition. */
    public long fetchCommittedOffset(String group, String topic, int partition) throws IOException {
        ByteBuffer request = Protocol.encodeOffsetFetchRequest(group, topic, partition);
        return withRetries(() -> Protocol.decodeOffsetFetchResponse(anyConnection().send(request)));
    }

    // ------------------------------------------------------------------ plumbing

    private <T> T withRetries(Request<T> request) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                sleep(retryBackoffMs * Math.min(attempt, 4));
                try {
                    refreshMetadata();
                } catch (IOException e) {
                    last = e;
                    continue;
                }
            }
            try {
                return request.run();
            } catch (KafkaException e) {
                if (!e.isRetriable()) {
                    throw e;
                }
                last = e;
            } catch (IOException e) {
                last = e; // broker unreachable: it may have died, so refresh metadata and retry
            }
        }
        throw last;
    }

    private BrokerConnection leaderConnection(String topic, int partition) throws IOException {
        Protocol.TopicMetadata t = metadata.topic(topic);
        Protocol.PartitionMetadata p = t == null ? null : t.partition(partition);
        if (p == null) {
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic + "-" + partition);
        }
        BrokerInfo leader = metadata.broker(p.leader());
        if (leader == null) {
            throw new KafkaException(Errors.LEADER_NOT_AVAILABLE, "No live leader for " + topic + "-" + partition);
        }
        return connection(leader);
    }

    private BrokerConnection anyConnection() throws IOException {
        if (!metadata.brokers().isEmpty()) {
            return connection(metadata.brokers().get(0));
        }
        InetSocketAddress a = bootstrapServers.get(0);
        return connection(a.getHostString(), a.getPort());
    }

    private BrokerConnection connection(BrokerInfo broker) {
        return connection(broker.getHost(), broker.getPort());
    }

    private BrokerConnection connection(String host, int port) {
        return connections.computeIfAbsent(host + ":" + port, k -> new BrokerConnection(host, port, requestTimeoutMs));
    }

    private static List<InetSocketAddress> parseServers(String servers) {
        List<InetSocketAddress> out = new ArrayList<>();
        for (String s : servers.split(",")) {
            String trimmed = s.trim();
            int colon = trimmed.lastIndexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("Expected host:port but got '" + trimmed + "'");
            }
            out.add(InetSocketAddress.createUnresolved(trimmed.substring(0, colon), Integer.parseInt(trimmed.substring(colon + 1))));
        }
        return out;
    }

    private static void sleep(long ms) throws InterruptedIOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
    }

    @Override
    public void close() {
        connections.values().forEach(BrokerConnection::close);
        connections.clear();
    }
}
