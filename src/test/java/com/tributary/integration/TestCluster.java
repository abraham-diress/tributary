package com.tributary.integration;

import com.tributary.broker.BrokerConfig;
import com.tributary.broker.TributaryBroker;
import com.tributary.client.TributaryClient;
import com.tributary.common.Protocol;
import com.tributary.tools.EmbeddedZooKeeper;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/** ZooKeeper plus N brokers in-process, on ephemeral ports, with a scratch data directory. */
final class TestCluster implements AutoCloseable {
    private final File dataDir;
    private final EmbeddedZooKeeper zk;
    private final Map<Integer, TributaryBroker> brokers = new TreeMap<>();
    private final Map<Integer, Integer> ports = new TreeMap<>();

    TestCluster(File dataDir, int numBrokers) throws Exception {
        this.dataDir = dataDir;
        this.zk = new EmbeddedZooKeeper(0, new File(dataDir, "zk"));
        for (int id = 1; id <= numBrokers; id++) {
            startBroker(id);
        }
        waitUntil(() -> controllerCount() == 1, 10_000, "a controller to be elected");
    }

    TributaryBroker startBroker(int id) throws Exception {
        BrokerConfig config = new BrokerConfig(id, "127.0.0.1", ports.getOrDefault(id, 0), zk.connectString(), dataDir.getPath());
        config.replicaLagMaxMs = 3000;
        config.replicaFetchWaitMs = 200;
        TributaryBroker broker = new TributaryBroker(config);
        broker.start();
        brokers.put(id, broker);
        ports.put(id, broker.getPort());
        return broker;
    }

    void stopBroker(int id) {
        TributaryBroker b = brokers.remove(id);
        if (b != null) {
            b.stop();
        }
    }

    TributaryBroker broker(int id) {
        return brokers.get(id);
    }

    long controllerCount() {
        return brokers.values().stream().filter(TributaryBroker::isController).count();
    }

    int controllerId() {
        return brokers.values().stream().filter(TributaryBroker::isController).findFirst()
                .map(TributaryBroker::getBrokerId).orElse(-1);
    }

    String bootstrap() {
        return brokers.values().stream().map(b -> "127.0.0.1:" + b.getPort()).collect(Collectors.joining(","));
    }

    Protocol.PartitionMetadata partitionMetadata(String topic, int partition) throws Exception {
        try (TributaryClient client = new TributaryClient(bootstrap())) {
            client.initialize();
            return client.metadata().topic(topic).partition(partition);
        }
    }

    static void waitUntil(BooleanSupplier condition, long timeoutMs, String description) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timed out waiting for " + description);
            }
            Thread.sleep(50);
        }
    }

    @Override
    public void close() {
        brokers.keySet().stream().toList().forEach(this::stopBroker);
        zk.close();
    }
}
