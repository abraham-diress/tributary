package com.tributary.client;

import com.tributary.common.OffsetOutOfRangeException;
import com.tributary.common.Protocol;
import com.tributary.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pull-based consumer. It tracks a position (next offset to read) per partition, long-polls the
 * partition leaders, and can commit positions for a consumer group so a restarted consumer resumes
 * where the group left off. Partitions are assigned statically: either one explicit partition or
 * all partitions of the topic.
 */
public class TributaryConsumer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TributaryConsumer.class);
    private static final int MAX_BYTES = 1024 * 1024;
    private static final long POLL_INTERVAL_MS = 100;

    private final TributaryClient client;
    private final String topic;
    private final String groupId;
    private final Integer fixedPartition;
    private final boolean resetToEarliest;
    private final List<Integer> assignment = new ArrayList<>();
    private final Map<Integer, Long> positions = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;
    private int nextPartitionIndex;

    /** Consumes a single partition from offset 0, without a group (as in the tutorial). */
    public TributaryConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(bootstrapBroker + ":" + bootstrapPort, null, topic, partition, true);
    }

    /** Consumes every partition of the topic as part of {@code groupId}. */
    public TributaryConsumer(String bootstrapServers, String groupId, String topic) {
        this(bootstrapServers, groupId, topic, null, true);
    }

    /**
     * @param partition       a single partition to consume, or null for all partitions
     * @param resetToEarliest where to start with no committed offset: earliest (true) or latest (false)
     */
    public TributaryConsumer(String bootstrapServers, String groupId, String topic, Integer partition, boolean resetToEarliest) {
        this.client = new TributaryClient(bootstrapServers);
        this.groupId = groupId;
        this.topic = topic;
        this.fixedPartition = partition;
        this.resetToEarliest = resetToEarliest;
    }

    public void initialize() throws IOException {
        client.initialize();
        assignment.clear();
        if (fixedPartition != null) {
            assignment.add(fixedPartition);
        } else {
            int n = client.partitionCount(topic);
            for (int p = 0; p < n; p++) {
                assignment.add(p);
            }
        }
        for (int p : assignment) {
            long committed = groupId == null ? -1 : client.fetchCommittedOffset(groupId, topic, p);
            positions.put(p, committed >= 0 ? committed : resetPosition(p));
        }
        LOG.debug("Consumer for '{}' starting at positions {}", topic, positions);
    }

    private long resetPosition(int partition) throws IOException {
        long[] range = client.offsetRange(topic, partition);
        return resetToEarliest ? range[0] : range[1];
    }

    /**
     * Returns the next batch of records from any assigned partition, waiting up to {@code timeout}
     * for data to arrive.
     */
    public List<ConsumerRecord> poll(Duration timeout) throws IOException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int n = assignment.size();
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            List<ConsumerRecord> out = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int p = assignment.get((nextPartitionIndex + i) % n);
                // With one partition, let the broker hold the request (long poll). With several,
                // check each without waiting so a quiet partition can't delay a busy one.
                int maxWait = n == 1 ? (int) Math.max(0, Math.min(remaining, 10_000)) : 0;
                out.addAll(fetchPartition(p, maxWait));
            }
            nextPartitionIndex = (nextPartitionIndex + 1) % Math.max(1, n);
            if (!out.isEmpty() || System.currentTimeMillis() >= deadline) {
                return out;
            }
            if (n > 1) {
                sleep(Math.min(20, Math.max(1, deadline - System.currentTimeMillis())));
            }
        }
    }

    /** Tutorial-style poll: values from the next batch, waiting briefly if nothing is available. */
    public List<byte[]> poll() throws IOException {
        List<byte[]> values = new ArrayList<>();
        for (ConsumerRecord r : poll(Duration.ofMillis(POLL_INTERVAL_MS))) {
            values.add(r.value());
        }
        return values;
    }

    private List<ConsumerRecord> fetchPartition(int partition, int maxWaitMs) throws IOException {
        long position = positions.get(partition);
        try {
            Protocol.FetchResult result = client.fetch(topic, partition, position, MAX_BYTES, maxWaitMs);
            List<ConsumerRecord> out = new ArrayList<>();
            for (Record r : result.records()) {
                if (r.offset() >= position) {
                    out.add(new ConsumerRecord(topic, partition, r.offset(), r.timestamp(), r.key(), r.value()));
                }
            }
            if (!out.isEmpty()) {
                positions.put(partition, out.get(out.size() - 1).offset() + 1);
            }
            return out;
        } catch (OffsetOutOfRangeException e) {
            long reset = resetToEarliest ? e.logStartOffset() : e.logEndOffset();
            LOG.warn("Offset {} out of range for {}-{}, resetting to {}", position, topic, partition, reset);
            positions.put(partition, reset);
            return List.of();
        }
    }

    /** Stores the current positions as the group's committed offsets. */
    public void commitSync() throws IOException {
        if (groupId == null) {
            throw new IllegalStateException("commitSync requires a consumer group");
        }
        for (Map.Entry<Integer, Long> e : positions.entrySet()) {
            client.commitOffset(groupId, topic, e.getKey(), e.getValue());
        }
    }

    /** Runs a background loop that hands every record to {@code handler}. */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        for (ConsumerRecord record : poll(Duration.ofMillis(500))) {
                            handler.handle(record);
                        }
                    } catch (Exception e) {
                        if (running.get()) {
                            LOG.error("Error in consumer loop, retrying", e);
                            sleep(1000);
                        }
                    }
                }
            }, "consumer-" + topic);
            consumerThread.setDaemon(true);
            consumerThread.start();
            LOG.info("Started consuming from topic {} partitions {}", topic, assignment);
        }
    }

    public void stopConsuming() {
        if (running.compareAndSet(true, false) && consumerThread != null) {
            try {
                consumerThread.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void seek(int partition, long offset) {
        positions.put(partition, offset);
    }

    /** For the single-partition constructor. */
    public void seek(long offset) {
        seek(assignment.isEmpty() ? fixedPartition : assignment.get(0), offset);
    }

    public long position(int partition) {
        return positions.getOrDefault(partition, -1L);
    }

    public long getCurrentOffset() {
        return position(assignment.isEmpty() ? fixedPartition : assignment.get(0));
    }

    public List<Integer> assignment() {
        return List.copyOf(assignment);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stopConsuming();
        client.close();
    }

    /** Continuously polls one partition and prints every message (tutorial step 8). */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TributaryConsumer <bootstrapHost> <bootstrapPort> <topic> <partition>");
            System.exit(1);
        }
        try (TributaryConsumer consumer = new TributaryConsumer(args[0], Integer.parseInt(args[1]), args[2],
                Integer.parseInt(args[3]))) {
            consumer.initialize();
            System.out.printf("Consuming %s-%s from offset %d (Ctrl+C to stop)%n", args[2], args[3], consumer.getCurrentOffset());
            while (true) {
                for (ConsumerRecord r : consumer.poll(Duration.ofSeconds(1))) {
                    System.out.printf("offset=%d key=%s value=%s%n", r.offset(), r.keyAsString(), r.valueAsString());
                }
            }
        }
    }
}
