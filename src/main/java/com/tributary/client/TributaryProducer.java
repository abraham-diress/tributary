package com.tributary.client;

import com.tributary.common.Protocol;
import com.tributary.common.TopicPartition;
import com.tributary.storage.Record;
import com.tributary.storage.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Asynchronous, batching producer.
 *
 * <p>{@link #send(String, byte[], byte[])} only appends the record to an in-memory batch for its
 * partition and returns a future. A background sender ships a batch once it is full
 * ({@code batchSizeBytes}) or has waited {@code lingerMs}, trading a few milliseconds of latency
 * for far fewer requests. At most one batch per partition is in flight, which preserves ordering
 * within a partition while different partitions are sent in parallel.
 *
 * <p>Records with a key always go to the same partition (murmur2 hash of the key, as in Kafka), so
 * per-key ordering is preserved. Records without a key are spread round-robin.
 */
public class TributaryProducer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TributaryProducer.class);

    public static final class Config {
        /** 0 = don't wait, 1 = leader has written it, -1 = all in-sync replicas have it. */
        public short acks = Protocol.ACKS_LEADER;
        public int lingerMs = 5;
        public int batchSizeBytes = 64 * 1024;
        /** send() blocks once this many bytes are waiting to be sent (back-pressure). */
        public long bufferMemoryBytes = 32L * 1024 * 1024;
        public int maxInFlightThreads = 8;
    }

    private static final class Batch {
        final TopicPartition tp;
        final long createdMs;
        final List<Record> records = new ArrayList<>();
        final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        int bytes;

        Batch(TopicPartition tp, long createdMs) {
            this.tp = tp;
            this.createdMs = createdMs;
        }
    }

    private final TributaryClient client;
    private final String defaultTopic;
    private final Config config;
    private final Map<TopicPartition, Deque<Batch>> batches = new HashMap<>();
    private final Set<TopicPartition> inFlight = new HashSet<>();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final ExecutorService ioPool;
    private Thread sender;
    private long queuedBytes;
    private int flushesInProgress;
    private boolean closed;
    private volatile boolean initialized;

    public TributaryProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this(bootstrapBroker + ":" + bootstrapPort, topic, new Config());
    }

    public TributaryProducer(String bootstrapServers, Config config) {
        this(bootstrapServers, null, config);
    }

    public TributaryProducer(String bootstrapServers, String defaultTopic, Config config) {
        this.client = new TributaryClient(bootstrapServers);
        this.defaultTopic = defaultTopic;
        this.config = config;
        this.ioPool = Executors.newFixedThreadPool(config.maxInFlightThreads, r -> {
            Thread t = new Thread(r, "producer-io");
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void initialize() throws IOException {
        if (initialized) {
            return;
        }
        client.initialize();
        sender = new Thread(this::runSender, "producer-sender");
        sender.setDaemon(true);
        sender.start();
        initialized = true;
    }

    public TributaryClient client() {
        return client;
    }

    // ------------------------------------------------------------------ sync API (as in the tutorial)

    /** Sends to a random-ish partition of the default topic and waits for the offset. */
    public long send(String message) throws IOException {
        return await(send(defaultTopic, null, null, message.getBytes(StandardCharsets.UTF_8))).offset();
    }

    public long send(String message, int partition) throws IOException {
        return await(send(defaultTopic, partition, null, message.getBytes(StandardCharsets.UTF_8))).offset();
    }

    // ------------------------------------------------------------------ async API

    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) throws IOException {
        return send(topic, null, key, value);
    }

    public CompletableFuture<RecordMetadata> send(String topic, String key, String value) throws IOException {
        return send(topic, null, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public CompletableFuture<RecordMetadata> send(String topic, Integer partition, byte[] key, byte[] value) throws IOException {
        if (topic == null) {
            throw new IllegalArgumentException("No topic given and no default topic configured");
        }
        initialize();
        int numPartitions = client.partitionCount(topic);
        int p = partition != null ? partition : choosePartition(key, numPartitions);
        Record record = new Record(-1, System.currentTimeMillis(), key, value);
        int size = Records.sizeInBytes(record);
        TopicPartition tp = new TopicPartition(topic, p);

        synchronized (this) {
            while (queuedBytes + size > config.bufferMemoryBytes && queuedBytes > 0 && !closed) {
                waitQuietly(0);
            }
            if (closed) {
                throw new IllegalStateException("Producer is closed");
            }
            Deque<Batch> queue = batches.computeIfAbsent(tp, k -> new ArrayDeque<>());
            Batch batch = queue.peekLast();
            boolean newBatch = batch == null || (batch.bytes + size > config.batchSizeBytes && !batch.records.isEmpty());
            if (newBatch) {
                batch = new Batch(tp, System.currentTimeMillis());
                queue.addLast(batch);
            }
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            batch.records.add(record);
            batch.futures.add(future);
            batch.bytes += size;
            queuedBytes += size;
            if (newBatch || batch.bytes >= config.batchSizeBytes) {
                notifyAll();
            }
            return future;
        }
    }

    int choosePartition(byte[] key, int numPartitions) {
        if (key == null) {
            return Math.floorMod(roundRobin.getAndIncrement(), numPartitions);
        }
        return (murmur2(key) & 0x7fffffff) % numPartitions;
    }

    /** Blocks until every record sent so far has been acknowledged (or failed). */
    public void flush() {
        synchronized (this) {
            flushesInProgress++;
            notifyAll();
            try {
                while (!allQueuesEmpty() || !inFlight.isEmpty()) {
                    waitQuietly(0);
                }
            } finally {
                flushesInProgress--;
            }
        }
    }

    // ------------------------------------------------------------------ sender

    private void runSender() {
        while (true) {
            List<Batch> ready;
            synchronized (this) {
                ready = drainReady();
                while (ready.isEmpty()) {
                    if (closed && allQueuesEmpty() && inFlight.isEmpty()) {
                        return;
                    }
                    waitQuietly(timeUntilNextBatchReady());
                    ready = drainReady();
                }
                for (Batch b : ready) {
                    inFlight.add(b.tp);
                }
            }
            for (Batch b : ready) {
                ioPool.execute(() -> sendBatch(b));
            }
        }
    }

    /** Caller holds the monitor. Takes at most one ready batch per partition that has none in flight. */
    private List<Batch> drainReady() {
        List<Batch> ready = new ArrayList<>();
        long now = System.currentTimeMillis();
        boolean force = closed || flushesInProgress > 0;
        for (Map.Entry<TopicPartition, Deque<Batch>> e : batches.entrySet()) {
            Deque<Batch> queue = e.getValue();
            if (queue.isEmpty() || inFlight.contains(e.getKey())) {
                continue;
            }
            Batch head = queue.peekFirst();
            if (force || queue.size() > 1 || head.bytes >= config.batchSizeBytes || now - head.createdMs >= config.lingerMs) {
                ready.add(queue.pollFirst());
            }
        }
        return ready;
    }

    private long timeUntilNextBatchReady() {
        long now = System.currentTimeMillis();
        long next = Long.MAX_VALUE;
        for (Map.Entry<TopicPartition, Deque<Batch>> e : batches.entrySet()) {
            if (!e.getValue().isEmpty() && !inFlight.contains(e.getKey())) {
                next = Math.min(next, e.getValue().peekFirst().createdMs + config.lingerMs - now);
            }
        }
        return next == Long.MAX_VALUE ? 0 : Math.max(1, next);
    }

    private void sendBatch(Batch batch) {
        try {
            long baseOffset = client.produce(batch.tp.topic(), batch.tp.partition(), batch.records, config.acks);
            for (int i = 0; i < batch.futures.size(); i++) {
                batch.futures.get(i).complete(new RecordMetadata(batch.tp.topic(), batch.tp.partition(), baseOffset + i));
            }
        } catch (Exception e) {
            LOG.warn("Failed to send batch of {} record(s) to {}: {}", batch.records.size(), batch.tp, e.getMessage());
            for (CompletableFuture<RecordMetadata> f : batch.futures) {
                f.completeExceptionally(e);
            }
        } finally {
            synchronized (this) {
                inFlight.remove(batch.tp);
                queuedBytes -= batch.bytes;
                notifyAll();
            }
        }
    }

    private boolean allQueuesEmpty() {
        for (Deque<Batch> q : batches.values()) {
            if (!q.isEmpty()) return false;
        }
        return true;
    }

    private void waitQuietly(long ms) {
        try {
            wait(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static RecordMetadata await(CompletableFuture<RecordMetadata> future) throws IOException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    /** Kafka's murmur2 hash, so keys map to partitions the same way a real Kafka producer would. */
    static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            notifyAll();
        }
        if (sender != null) {
            try {
                sender.join(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        flush();
        ioPool.shutdown();
        try {
            ioPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        client.close();
    }

    // ------------------------------------------------------------------ main (tutorial step 8)

    /** Creates the topic if needed, sends 10 test messages and prints where each one landed. */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TributaryProducer <bootstrapHost> <bootstrapPort> <topic> [count]");
            System.exit(1);
        }
        String topic = args[2];
        int count = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        Config config = new Config();
        config.acks = Protocol.ACKS_ALL;
        try (TributaryProducer producer = new TributaryProducer(args[0] + ":" + args[1], topic, config)) {
            TributaryClient client = producer.client();
            client.initialize();
            if (!client.topicExists(topic)) {
                int brokers = client.metadata().brokers().size();
                int rf = Math.min(3, brokers);
                System.out.printf("Creating topic '%s' with 3 partitions, replication factor %d%n", topic, rf);
                client.createTopic(topic, 3, rf);
            }
            for (int i = 0; i < count; i++) {
                String message = "Message " + i + " @ " + System.currentTimeMillis();
                RecordMetadata md = producer.send(topic, "key-" + i, message).get();
                System.out.printf("Sent '%s' -> partition %d, offset %d%n", message, md.partition(), md.offset());
            }
        }
    }
}
