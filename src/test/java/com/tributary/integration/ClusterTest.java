package com.tributary.integration;

import com.tributary.broker.TributaryBroker;
import com.tributary.client.TributaryClient;
import com.tributary.client.TributaryConsumer;
import com.tributary.client.TributaryProducer;
import com.tributary.tools.EmbeddedZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs ZooKeeper and three brokers in one JVM. */
@Timeout(120)
class ClusterTest {
    @TempDir
    File dataDir;
    private EmbeddedZooKeeper zookeeper;
    private final Map<Integer, TributaryBroker> brokers = new TreeMap<>();
    private final Map<Integer, Integer> ports = new TreeMap<>();

    @BeforeEach
    void startCluster() throws Exception {
        zookeeper = new EmbeddedZooKeeper(0, new File(dataDir, "zk"));
        for (int id = 1; id <= 3; id++) {
            startBroker(id);
        }
    }

    @AfterEach
    void stopCluster() {
        brokers.values().forEach(TributaryBroker::stop);
        zookeeper.close();
    }

    private void startBroker(int id) throws Exception {
        TributaryBroker broker = new TributaryBroker(id, "127.0.0.1", ports.getOrDefault(id, 0),
                zookeeper.getConnectString(), dataDir.getPath());
        broker.start();
        brokers.put(id, broker);
        ports.put(id, broker.getPort());
    }

    private void stopBroker(int id) {
        brokers.remove(id).stop();
    }

    private int anyPort() {
        return brokers.values().iterator().next().getPort();
    }

    private int leaderOf(String topic, int partition) throws Exception {
        TributaryClient client = new TributaryClient("127.0.0.1", anyPort());
        client.initialize();
        return client.getTopicMetadata(topic).getPartitions().get(partition).getLeader();
    }

    private List<String> readAll(String topic, int partition) throws Exception {
        TributaryConsumer consumer = new TributaryConsumer("127.0.0.1", anyPort(), topic, partition);
        consumer.initialize();
        List<String> result = new ArrayList<>();
        List<byte[]> batch;
        while (!(batch = consumer.poll()).isEmpty()) {
            for (byte[] message : batch) {
                result.add(new String(message, StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    private static void waitUntil(java.util.concurrent.Callable<Boolean> condition, String what) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!condition.call()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(100);
        }
    }

    @Test
    void oneBrokerIsElectedController() throws Exception {
        waitUntil(() -> brokers.values().stream().filter(TributaryBroker::isController).count() == 1, "a controller");
    }

    @Test
    void producesAndConsumesAcrossPartitions() throws Exception {
        TributaryClient client = new TributaryClient("127.0.0.1", anyPort());
        client.initialize();
        client.createTopic("orders", 3, 3);

        TributaryProducer producer = new TributaryProducer("127.0.0.1", anyPort(), "orders");
        producer.initialize();
        for (int i = 0; i < 30; i++) {
            producer.send("order-" + i, i % 3);
        }

        int total = 0;
        for (int p = 0; p < 3; p++) {
            List<String> messages = readAll("orders", p);
            assertEquals(10, messages.size());
            assertEquals("order-" + p, messages.get(0));
            total += messages.size();
        }
        assertEquals(30, total);
    }

    @Test
    void replicatesEveryMessageToFollowers() throws Exception {
        TributaryClient client = new TributaryClient("127.0.0.1", anyPort());
        client.initialize();
        client.createTopic("events", 1, 3);
        for (int i = 0; i < 50; i++) {
            client.send("events", 0, ("event-" + i).getBytes(StandardCharsets.UTF_8));
        }
        for (TributaryBroker broker : brokers.values()) {
            assertEquals(50, broker.getPartition("events", 0).getNextOffset(), "broker " + broker.getBrokerId());
        }
    }

    @Test
    void newLeaderIsElectedAndNoMessagesAreLostWhenTheLeaderStops() throws Exception {
        TributaryClient client = new TributaryClient("127.0.0.1", anyPort());
        client.initialize();
        client.createTopic("payments", 1, 3);
        for (int i = 0; i < 20; i++) {
            client.send("payments", 0, ("payment-" + i).getBytes(StandardCharsets.UTF_8));
        }

        int oldLeader = leaderOf("payments", 0);
        stopBroker(oldLeader);
        waitUntil(() -> {
            int leader = leaderOf("payments", 0);
            return leader != -1 && leader != oldLeader;
        }, "a new leader");

        for (int i = 20; i < 40; i++) {
            client.send("payments", 0, ("payment-" + i).getBytes(StandardCharsets.UTF_8));
        }
        List<String> messages = readAll("payments", 0);
        assertEquals(40, messages.size());
        for (int i = 0; i < 40; i++) {
            assertEquals("payment-" + i, messages.get(i));
        }

        // The old leader comes back as a follower and is sent the messages it missed
        startBroker(oldLeader);
        client.send("payments", 0, "payment-40".getBytes(StandardCharsets.UTF_8));
        assertEquals(41, brokers.get(oldLeader).getPartition("payments", 0).getNextOffset());
    }

    @Test
    void anotherBrokerTakesOverWhenTheControllerStops() throws Exception {
        waitUntil(() -> brokers.values().stream().filter(TributaryBroker::isController).count() == 1, "a controller");
        int oldController = brokers.values().stream().filter(TributaryBroker::isController).findFirst().get().getBrokerId();
        stopBroker(oldController);
        waitUntil(() -> brokers.values().stream().filter(TributaryBroker::isController).count() == 1, "a new controller");

        TributaryClient client = new TributaryClient("127.0.0.1", anyPort());
        client.initialize();
        client.createTopic("after-failover", 2, 2);
        assertNotEquals(-1, leaderOf("after-failover", 1));
        assertTrue(client.send("after-failover", 1, "works".getBytes(StandardCharsets.UTF_8)) >= 0);
    }
}

