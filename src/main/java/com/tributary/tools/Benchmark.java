package com.tributary.tools;

import com.tributary.client.ConsumerRecord;
import com.tributary.client.TributaryClient;
import com.tributary.client.TributaryConsumer;
import com.tributary.client.TributaryProducer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer and consumer throughput benchmark.
 *
 * <pre>
 *   Benchmark [--bootstrap localhost:9091] [--topic bench] [--records 1000000] [--record-size 100]
 *             [--partitions 6] [--replication-factor 3] [--acks 1] [--linger-ms 5] [--batch-size 65536]
 * </pre>
 */
public final class Benchmark {
    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        String[] withCommand = new String[args.length + 1];
        withCommand[0] = "bench";
        System.arraycopy(args, 0, withCommand, 1, args.length);
        Map<String, String> o = Cli.parse(withCommand);
        String bootstrap = o.getOrDefault("bootstrap", "localhost:9091");
        String topic = o.getOrDefault("topic", "bench-" + System.currentTimeMillis());
        int records = Integer.parseInt(o.getOrDefault("records", "1000000"));
        int recordSize = Integer.parseInt(o.getOrDefault("record-size", "100"));
        int partitions = Integer.parseInt(o.getOrDefault("partitions", "6"));
        int rf = Integer.parseInt(o.getOrDefault("replication-factor", "3"));

        TributaryProducer.Config config = new TributaryProducer.Config();
        config.acks = Cli.parseAcks(o.getOrDefault("acks", "1"));
        config.lingerMs = Integer.parseInt(o.getOrDefault("linger-ms", "5"));
        config.batchSizeBytes = Integer.parseInt(o.getOrDefault("batch-size", "65536"));

        try (TributaryClient admin = new TributaryClient(bootstrap)) {
            admin.initialize();
            if (!admin.topicExists(topic)) {
                admin.createTopic(topic, partitions, rf);
            }
        }

        byte[] payload = new byte[recordSize];
        new Random(42).nextBytes(payload);
        long[] latencies = new long[records];
        AtomicLong failures = new AtomicLong();
        CountDownLatch done = new CountDownLatch(records);

        System.out.printf("Producing %,d records of %d bytes to '%s' (%d partitions, RF %d, acks=%s, linger=%dms, batch=%dB)%n",
                records, recordSize, topic, partitions, rf, o.getOrDefault("acks", "1"), config.lingerMs, config.batchSizeBytes);
        long start = System.nanoTime();
        try (TributaryProducer producer = new TributaryProducer(bootstrap, config)) {
            for (int i = 0; i < records; i++) {
                final int idx = i;
                final long sendStart = System.nanoTime();
                producer.send(topic, (byte[]) null, payload).whenComplete((md, err) -> {
                    latencies[idx] = System.nanoTime() - sendStart;
                    if (err != null) failures.incrementAndGet();
                    done.countDown();
                });
            }
            done.await();
        }
        double secs = (System.nanoTime() - start) / 1e9;
        Arrays.sort(latencies);
        System.out.printf("PRODUCE: %,.0f records/sec, %.1f MB/sec, avg latency %.1f ms, p50 %.1f ms, p99 %.1f ms, failures %d%n",
                records / secs, records * (double) recordSize / secs / (1024 * 1024),
                Arrays.stream(latencies).average().orElse(0) / 1e6, latencies[records / 2] / 1e6,
                latencies[(int) (records * 0.99)] / 1e6, failures.get());

        start = System.nanoTime();
        long consumed = 0;
        try (TributaryConsumer consumer = new TributaryConsumer(bootstrap, null, topic, null, true)) {
            consumer.initialize();
            long idleDeadline = System.currentTimeMillis() + 10_000;
            while (consumed < records && System.currentTimeMillis() < idleDeadline) {
                var batch = consumer.poll(Duration.ofMillis(500));
                if (!batch.isEmpty()) {
                    consumed += batch.size();
                    idleDeadline = System.currentTimeMillis() + 10_000;
                }
            }
        }
        secs = (System.nanoTime() - start) / 1e9;
        System.out.printf("CONSUME: %,d records in %.2fs = %,.0f records/sec, %.1f MB/sec%n",
                consumed, secs, consumed / secs, consumed * (double) recordSize / secs / (1024 * 1024));
    }
}
