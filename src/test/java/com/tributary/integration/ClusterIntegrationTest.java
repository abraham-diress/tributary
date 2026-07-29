package com.tributary.integration;

import com.tributary.broker.Partition;
import com.tributary.client.ConsumerRecord;
import com.tributary.client.RecordMetadata;
import com.tributary.client.TributaryClient;
import com.tributary.client.TributaryConsumer;
import com.tributary.client.TributaryProducer;
import com.tributary.common.Errors;
import com.tributary.common.KafkaException;
import com.tributary.common.Protocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class ClusterIntegrationTest {
    @TempDir
    File dataDir;
    private TestCluster cluster;

    @BeforeEach
    void setUp() throws Exception {
        cluster = new TestCluster(dataDir, 3);
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    private TributaryProducer producer(short acks) throws Exception {
        TributaryProducer.Config config = new TributaryProducer.Config();
        config.acks = acks;
        TributaryProducer producer = new TributaryProducer(cluster.bootstrap(), config);
        producer.initialize();
        return producer;
    }

    private void createTopic(String topic, int partitions, int rf) throws Exception {
        try (TributaryClient client = new TributaryClient(cluster.bootstrap())) {
            client.initialize();
            client.createTopic(topic, partitions, rf);
        }
    }

    private List<ConsumerRecord> consumeAll(TributaryConsumer consumer, int expected) throws Exception {
        List<ConsumerRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (out.size() < expected && System.currentTimeMillis() < deadline) {
            out.addAll(consumer.poll(Duration.ofMillis(200)));
        }
        return out;
    }

    @Test
    void producesAndConsumesAcrossReplicatedPartitions() throws Exception {
        createTopic("orders", 3, 3);
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        try (TributaryProducer producer = producer(Protocol.ACKS_ALL)) {
            for (int i = 0; i < 300; i++) {
                futures.add(producer.send("orders", "customer-" + (i % 10), "order-" + i));
            }
            producer.flush();
        }
        for (CompletableFuture<RecordMetadata> f : futures) {
            f.get();
        }

        try (TributaryConsumer consumer = new TributaryConsumer(cluster.bootstrap(), null, "orders", null, true)) {
            consumer.initialize();
            List<ConsumerRecord> records = consumeAll(consumer, 300);
            assertEquals(300, records.size());

            // Same key -> same partition, in send order.
            Map<String, Integer> partitionForKey = new HashMap<>();
            Map<String, Integer> lastSeq = new HashMap<>();
            for (ConsumerRecord r : records) {
                Integer previous = partitionForKey.putIfAbsent(r.keyAsString(), r.partition());
                assertTrue(previous == null || previous == r.partition(), "key moved partitions");
            }
            records.sort((a, b) -> Long.compare(a.offset(), b.offset()));
            for (ConsumerRecord r : records) {
                int seq = Integer.parseInt(r.valueAsString().substring("order-".length()));
                Integer prev = lastSeq.put(r.keyAsString(), seq);
                if (prev != null && partitionForKey.get(r.keyAsString()) == r.partition()) {
                    assertTrue(seq > prev, "per-key ordering violated");
                }
            }
        }

        // acks=all means every in-sync replica has every record.
        for (int p = 0; p < 3; p++) {
            Protocol.PartitionMetadata md = cluster.partitionMetadata("orders", p);
            long leaderEnd = cluster.broker(md.leader()).getPartition("orders", p).log().logEndOffset();
            for (int replica : md.replicas()) {
                final int partitionId = p;
                TestCluster.waitUntil(() -> cluster.broker(replica).getPartition("orders", partitionId).log().logEndOffset() == leaderEnd,
                        5000, "replica " + replica + " to catch up");
            }
        }
    }

    @Test
    void survivesLeaderFailureWithoutLosingAcknowledgedWrites() throws Exception {
        createTopic("payments", 1, 3);
        try (TributaryProducer producer = producer(Protocol.ACKS_ALL)) {
            for (int i = 0; i < 100; i++) {
                producer.send("payments", null, "payment-" + i).get();
            }
            int oldLeader = cluster.partitionMetadata("payments", 0).leader();
            cluster.stopBroker(oldLeader);

            TestCluster.waitUntil(() -> {
                try {
                    int leader = cluster.partitionMetadata("payments", 0).leader();
                    return leader >= 0 && leader != oldLeader;
                } catch (Exception e) {
                    return false;
                }
            }, 10_000, "a new leader");
            Protocol.PartitionMetadata md = cluster.partitionMetadata("payments", 0);
            assertNotEquals(oldLeader, md.leader());
            assertEquals(1, md.leaderEpoch());

            // The producer's client discovers the new leader on its own.
            for (int i = 100; i < 200; i++) {
                producer.send("payments", null, "payment-" + i).get();
            }

            try (TributaryConsumer consumer = new TributaryConsumer(cluster.bootstrap(), null, "payments", 0, true)) {
                consumer.initialize();
                List<ConsumerRecord> records = consumeAll(consumer, 200);
                assertEquals(200, records.size());
                for (int i = 0; i < 200; i++) {
                    assertEquals(i, records.get(i).offset());
                    assertEquals("payment-" + i, records.get(i).valueAsString());
                }
            }

            // The old leader comes back as a follower, catches up and rejoins the ISR.
            cluster.startBroker(oldLeader);
            TestCluster.waitUntil(() -> {
                try {
                    return cluster.partitionMetadata("payments", 0).isr().size() == 3;
                } catch (Exception e) {
                    return false;
                }
            }, 20_000, "the restarted broker to rejoin the ISR");
            Partition rejoined = cluster.broker(oldLeader).getPartition("payments", 0);
            assertEquals(200, rejoined.log().logEndOffset());
        }
    }

    @Test
    void controllerFailoverElectsNewController() throws Exception {
        int oldController = cluster.controllerId();
        cluster.stopBroker(oldController);
        TestCluster.waitUntil(() -> cluster.controllerCount() == 1, 10_000, "a new controller");
        assertNotEquals(oldController, cluster.controllerId());
        createTopic("after-failover", 2, 2);
        assertEquals(2, cluster.partitionMetadata("after-failover", 1).replicas().size());
    }

    @Test
    void consumerGroupResumesFromCommittedOffset() throws Exception {
        createTopic("events", 1, 2);
        try (TributaryProducer producer = producer(Protocol.ACKS_LEADER)) {
            for (int i = 0; i < 50; i++) producer.send("events", null, "event-" + i);
            producer.flush();
        }
        try (TributaryConsumer first = new TributaryConsumer(cluster.bootstrap(), "analytics", "events")) {
            first.initialize();
            List<ConsumerRecord> got = consumeAll(first, 50);
            assertEquals(50, got.size());
            first.seek(0, 20);
            first.commitSync();
        }
        try (TributaryConsumer second = new TributaryConsumer(cluster.bootstrap(), "analytics", "events")) {
            second.initialize();
            List<ConsumerRecord> rest = consumeAll(second, 30);
            assertEquals(30, rest.size());
            assertEquals(20, rest.get(0).offset());
        }
    }

    @Test
    void rejectsInvalidTopicRequests() throws Exception {
        createTopic("dupe", 1, 1);
        try (TributaryClient client = new TributaryClient(cluster.bootstrap())) {
            client.initialize();
            KafkaException exists = assertThrows(KafkaException.class, () -> client.createTopic("dupe", 1, 1));
            assertEquals(Errors.TOPIC_ALREADY_EXISTS, exists.code());
            KafkaException rf = assertThrows(KafkaException.class, () -> client.createTopic("too-many", 1, 5));
            assertEquals(Errors.INVALID_REPLICATION_FACTOR, rf.code());
        }
    }

    @Test
    void longPollReturnsAsSoonAsDataArrives() throws Exception {
        createTopic("live", 1, 1);
        try (TributaryConsumer consumer = new TributaryConsumer(cluster.bootstrap(), null, "live", 0, true);
             TributaryProducer producer = producer(Protocol.ACKS_LEADER)) {
            consumer.initialize();
            Thread sender = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    producer.send("live", null, "hello").get();
                } catch (Exception ignored) {
                }
            });
            sender.start();
            long start = System.currentTimeMillis();
            List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(5));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals(1, records.size());
            assertTrue(elapsed < 3000, "long poll should return when data arrives, took " + elapsed + "ms");
            sender.join();
        }
    }
}
