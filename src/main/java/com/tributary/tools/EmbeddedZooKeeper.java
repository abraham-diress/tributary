package com.tributary.tools;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A single-node ZooKeeper server running in-process. Used by tests and the local cluster launcher
 * so you don't need a separate ZooKeeper install.
 */
public final class EmbeddedZooKeeper implements Closeable {
    private final ZooKeeperServer server;
    private final ServerCnxnFactory factory;
    private final int port;

    public EmbeddedZooKeeper(int port, File dataDir) throws IOException, InterruptedException {
        System.setProperty("zookeeper.admin.enableServer", "false");
        server = new ZooKeeperServer(new File(dataDir, "snapshots"), new File(dataDir, "txnlog"), 2000);
        factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", port), 1000);
        factory.startup(server);
        this.port = factory.getLocalPort();
    }

    public int port() {
        return port;
    }

    public String connectString() {
        return "127.0.0.1:" + port;
    }

    @Override
    public void close() {
        factory.shutdown();
        server.shutdown();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 2181;
        File dir = new File(args.length > 1 ? args[1] : "data/zookeeper");
        EmbeddedZooKeeper zk = new EmbeddedZooKeeper(port, dir);
        Runtime.getRuntime().addShutdownHook(new Thread(zk::close));
        System.out.println("ZooKeeper listening on " + zk.connectString() + " (data: " + dir.getAbsolutePath() + ")");
        Thread.currentThread().join();
    }
}
