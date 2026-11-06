package com.tributary.tools;

import com.tributary.broker.BrokerConfig;
import com.tributary.broker.TributaryBroker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Starts ZooKeeper plus N brokers in a single JVM for quick experiments:
 * ZooKeeper on 2181 and brokers 1..N on ports 9091..909N.
 */
public final class LocalCluster {
    private LocalCluster() {
    }

    public static void main(String[] args) throws Exception {
        int brokers = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String dataDir = args.length > 1 ? args[1] : "data";
        EmbeddedZooKeeper zk = new EmbeddedZooKeeper(2181, new File(dataDir, "zookeeper"));
        List<TributaryBroker> started = new ArrayList<>();
        for (int id = 1; id <= brokers; id++) {
            TributaryBroker broker = new TributaryBroker(new BrokerConfig(id, "localhost", 9090 + id, zk.connectString(), dataDir));
            broker.start();
            started.add(broker);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            started.forEach(TributaryBroker::stop);
            zk.close();
        }));
        System.out.printf("Local cluster up: ZooKeeper %s, %d broker(s) on localhost:9091..%d. Ctrl+C to stop.%n",
                zk.connectString(), brokers, 9090 + brokers);
        Thread.currentThread().join();
    }
}
