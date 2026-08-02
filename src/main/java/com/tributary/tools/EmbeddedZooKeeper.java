package com.tributary.tools;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Runs a single ZooKeeper server inside the JVM, so the cluster can be started without
 * installing ZooKeeper separately. Also used by the integration tests.
 */
public class EmbeddedZooKeeper {
    private final ZooKeeperServer server;
    private final ServerCnxnFactory factory;

    public EmbeddedZooKeeper(int port, File dataDir) throws IOException, InterruptedException {
        System.setProperty("zookeeper.admin.enableServer", "false");
        server = new ZooKeeperServer(new File(dataDir, "snapshots"), new File(dataDir, "logs"), 2000);
        factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", port), 100);
        factory.startup(server);
    }

    public int getPort() {
        return factory.getLocalPort();
    }

    public String getConnectString() {
        return "127.0.0.1:" + getPort();
    }

    public void close() {
        factory.shutdown();
        server.shutdown();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 2181;
        File dir = new File(args.length > 1 ? args[1] : "data/zookeeper");
        EmbeddedZooKeeper zk = new EmbeddedZooKeeper(port, dir);
        Runtime.getRuntime().addShutdownHook(new Thread(zk::close));
        System.out.println("ZooKeeper running on " + zk.getConnectString());
        Thread.currentThread().join();
    }
}

