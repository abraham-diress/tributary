package com.tributary.client;

import com.tributary.broker.BrokerInfo;
import com.tributary.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Low-level client. It asks any broker for cluster metadata, then sends each request
 * directly to the leader of the partition. If a request fails (for example, because the
 * leader went down), it refreshes the metadata and tries again.
 */
public class TributaryClient {
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_BACKOFF_MS = 1000;

    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, Protocol.TopicMetadata> topicMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();

    public TributaryClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    /** Fetches the list of brokers and topics, trying the bootstrap broker and then any known broker. */
    public synchronized void refreshMetadata() throws IOException {
        List<InetSocketAddress> candidates = new ArrayList<>();
        candidates.add(new InetSocketAddress(bootstrapBroker, bootstrapPort));
        for (BrokerInfo broker : brokers.values()) {
            candidates.add(new InetSocketAddress(broker.getHost(), broker.getPort()));
        }

        IOException lastError = null;
        for (InetSocketAddress address : candidates) {
            try {
                Protocol.MetadataResult result = Protocol.decodeMetadataResponse(
                        sendRequest(address, Protocol.encodeMetadataRequest()));
                if (result.hasError()) {
                    throw new IOException(result.getError());
                }
                brokers.clear();
                for (BrokerInfo broker : result.getBrokers()) {
                    brokers.put(broker.getId(), broker);
                }
                topicMetadata.clear();
                for (Protocol.TopicMetadata topic : result.getTopics()) {
                    topicMetadata.put(topic.getName(), topic);
                }
                return;
            } catch (IOException e) {
                lastError = e;
            }
        }
        throw new IOException("Could not fetch metadata from any broker", lastError);
    }

    /** Creates a topic and waits until every partition has a leader. */
    public void createTopic(String topic, int numPartitions, int replicationFactor) throws IOException {
        ByteBuffer response = sendRequest(new InetSocketAddress(bootstrapBroker, bootstrapPort),
                Protocol.encodeCreateTopicRequest(topic, numPartitions, (short) replicationFactor));
        String error = Protocol.decodeCreateTopicResponse(response);
        if (error != null) {
            throw new IOException("Could not create topic " + topic + ": " + error);
        }
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            refreshMetadata();
            Protocol.TopicMetadata metadata = topicMetadata.get(topic);
            if (metadata != null && metadata.getPartitions().size() == numPartitions) {
                return;
            }
            sleep(100);
        }
        throw new IOException("Topic " + topic + " was not ready in time");
    }

    /** Sends a message to a specific topic-partition and returns its offset. */
    public long send(String topic, int partition, byte[] message) throws IOException {
        String lastError = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                sleep(RETRY_BACKOFF_MS);
                refreshMetadata();
            }
            try {
                BrokerInfo leader = findLeader(topic, partition);
                Protocol.ProduceResult result = Protocol.decodeProduceResponse(
                        sendRequest(leader, Protocol.encodeProduceRequest(topic, partition, message)));
                if (!result.hasError()) {
                    return result.getOffset();
                }
                lastError = result.getError();
            } catch (IOException e) {
                lastError = e.getMessage();
            }
        }
        throw new IOException("Failed to send message to " + topic + "-" + partition + ": " + lastError);
    }

    /** Fetches messages from a topic-partition, starting at {@code offset}. */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        String lastError = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                sleep(RETRY_BACKOFF_MS);
                refreshMetadata();
            }
            try {
                BrokerInfo leader = findLeader(topic, partition);
                Protocol.FetchResult result = Protocol.decodeFetchResponse(
                        sendRequest(leader, Protocol.encodeFetchRequest(topic, partition, offset, maxBytes)));
                if (!result.hasError()) {
                    return result.getMessages();
                }
                lastError = result.getError();
            } catch (IOException e) {
                lastError = e.getMessage();
            }
        }
        throw new IOException("Failed to fetch from " + topic + "-" + partition + ": " + lastError);
    }

    public Protocol.TopicMetadata getTopicMetadata(String topic) {
        return topicMetadata.get(topic);
    }

    public int getBrokerCount() {
        return brokers.size();
    }

    private BrokerInfo findLeader(String topic, int partition) throws IOException {
        Protocol.TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            throw new IOException("Unknown topic: " + topic);
        }
        for (Protocol.PartitionMetadata p : metadata.getPartitions()) {
            if (p.getId() == partition) {
                BrokerInfo leader = brokers.get(p.getLeader());
                if (leader == null) {
                    throw new IOException("No leader available for " + topic + "-" + partition);
                }
                return leader;
            }
        }
        throw new IOException("Unknown partition: " + topic + "-" + partition);
    }

    private ByteBuffer sendRequest(BrokerInfo broker, ByteBuffer request) throws IOException {
        return sendRequest(new InetSocketAddress(broker.getHost(), broker.getPort()), request);
    }

    private ByteBuffer sendRequest(InetSocketAddress address, ByteBuffer request) throws IOException {
        try (SocketChannel channel = SocketChannel.open(address)) {
            Protocol.send(channel, request);
            return Protocol.receive(channel);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

