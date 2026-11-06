package com.tributary.broker;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Thin wrapper around the ZooKeeper client used for cluster coordination:
 * <ul>
 *   <li>ephemeral nodes for broker liveness ({@code /brokers/ids/<id>}) and controller election ({@code /controller})</li>
 *   <li>persistent nodes for topic and partition state ({@code /topics/...}) and consumer offsets ({@code /consumers/...})</li>
 *   <li>versioned (compare-and-set) updates so the controller and partition leaders can't clobber each other</li>
 *   <li>persistent watches, which are re-registered automatically if the session expires</li>
 * </ul>
 */
public class ZookeeperClient implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);
    private static final int MAX_RETRIES = 10;

    private final String connectString;
    private final int sessionTimeoutMs;
    private volatile ZooKeeper zooKeeper;
    private volatile CountDownLatch connectedSignal = new CountDownLatch(1);
    private volatile Runnable sessionReestablishedListener = () -> { };
    private volatile boolean closed;
    private final List<WatchRegistration> watches = new CopyOnWriteArrayList<>();

    private record WatchRegistration(String path, AddWatchMode mode, Watcher watcher) {
    }

    /** A node's data together with its version, for compare-and-set updates. */
    public record VersionedData(String data, int version) {
    }

    @FunctionalInterface
    private interface ZkCall<T> {
        T call() throws KeeperException, InterruptedException;
    }

    public ZookeeperClient(String connectString) {
        this(connectString, 6000);
    }

    public ZookeeperClient(String connectString, int sessionTimeoutMs) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, this);
        if (!connectedSignal.await(15, TimeUnit.SECONDS)) {
            zooKeeper.close();
            throw new IOException("Timed out connecting to ZooKeeper at " + connectString);
        }
        try {
            createPath("/brokers/ids");
            createPath("/topics");
            createPath("/consumers");
        } catch (KeeperException e) {
            throw new IOException("Failed to create base ZooKeeper paths", e);
        }
    }

    /** Called (on a background thread) after the session expired and a new one was established. */
    public void onSessionReestablished(Runnable listener) {
        this.sessionReestablishedListener = listener;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() != Event.EventType.None) {
            return;
        }
        switch (event.getState()) {
            case SyncConnected -> {
                connectedSignal.countDown();
                LOG.debug("Connected to ZooKeeper");
            }
            case Disconnected -> LOG.warn("Disconnected from ZooKeeper, waiting to reconnect");
            case Expired -> {
                if (!closed) {
                    LOG.warn("ZooKeeper session expired, establishing a new session");
                    Thread t = new Thread(this::reestablishSession, "zk-session-reestablish");
                    t.setDaemon(true);
                    t.start();
                }
            }
            default -> {
            }
        }
    }

    private void reestablishSession() {
        while (!closed) {
            try {
                ZooKeeper old = zooKeeper;
                if (old != null) {
                    old.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, this);
                if (!connectedSignal.await(15, TimeUnit.SECONDS)) {
                    continue;
                }
                for (WatchRegistration w : watches) {
                    zooKeeper.addWatch(w.path(), w.watcher(), w.mode());
                }
                LOG.info("Re-established ZooKeeper session");
                sessionReestablishedListener.run();
                return;
            } catch (Exception e) {
                LOG.error("Failed to re-establish ZooKeeper session, retrying", e);
                sleepQuietly(1000);
            }
        }
    }

    /** Retries operations that failed because the connection to ZooKeeper was briefly lost. */
    private <T> T retry(ZkCall<T> call) throws KeeperException, InterruptedException {
        for (int attempt = 0; ; attempt++) {
            try {
                return call.call();
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
                if (attempt >= MAX_RETRIES || closed) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    /** Creates a persistent node and any missing parents, with empty data. No-op if it exists. */
    public void createPath(String path) throws KeeperException, InterruptedException {
        if (path.equals("/") || exists(path)) {
            return;
        }
        int slash = path.lastIndexOf('/');
        if (slash > 0) {
            createPath(path.substring(0, slash));
        }
        try {
            retry(() -> zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        } catch (KeeperException.NodeExistsException ignored) {
            // created concurrently by another broker
        }
    }

    /** Creates the node (and parents) or overwrites its data if it already exists. */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        if (exists(path)) {
            retry(() -> zooKeeper.setData(path, bytes, -1));
            return;
        }
        int slash = path.lastIndexOf('/');
        if (slash > 0) {
            createPath(path.substring(0, slash));
        }
        try {
            retry(() -> zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        } catch (KeeperException.NodeExistsException e) {
            retry(() -> zooKeeper.setData(path, bytes, -1));
        }
    }

    /**
     * Creates an ephemeral node, which ZooKeeper deletes automatically when this session ends.
     * Returns false if the node already exists (e.g. someone else is already the controller).
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        try {
            retry(() -> zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL));
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return retry(() -> zooKeeper.exists(path, false)) != null;
    }

    /** Returns the node's data, or null if it doesn't exist. */
    public String getData(String path) throws KeeperException, InterruptedException {
        VersionedData vd = getDataWithVersion(path);
        return vd == null ? null : vd.data();
    }

    public VersionedData getDataWithVersion(String path) throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] data = retry(() -> zooKeeper.getData(path, false, stat));
            return new VersionedData(data == null ? "" : new String(data, StandardCharsets.UTF_8), stat.getVersion());
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    /** Compare-and-set: writes only if the node is still at {@code expectedVersion}. */
    public boolean setData(String path, String data, int expectedVersion) throws KeeperException, InterruptedException {
        try {
            retry(() -> zooKeeper.setData(path, data.getBytes(StandardCharsets.UTF_8), expectedVersion));
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return false;
        }
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return retry(() -> zooKeeper.getChildren(path, false));
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        }
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {
        try {
            retry(() -> {
                zooKeeper.delete(path, -1);
                return null;
            });
        } catch (KeeperException.NoNodeException ignored) {
            // already gone
        }
    }

    /** Applies several operations atomically. */
    public void multi(List<Op> ops) throws KeeperException, InterruptedException {
        retry(() -> zooKeeper.multi(ops));
    }

    /**
     * Registers a persistent watch that keeps firing for every change (unlike classic one-shot
     * watches). With {@code recursive}, it fires for node creations and data changes anywhere
     * under {@code path}; otherwise for the node itself and changes to its children.
     */
    public void addPersistentWatch(String path, boolean recursive, Consumer<WatchedEvent> listener)
            throws KeeperException, InterruptedException {
        AddWatchMode mode = recursive ? AddWatchMode.PERSISTENT_RECURSIVE : AddWatchMode.PERSISTENT;
        Watcher watcher = event -> {
            if (event.getType() != Event.EventType.None) {
                listener.accept(event);
            }
        };
        retry(() -> {
            zooKeeper.addWatch(path, watcher, mode);
            return null;
        });
        watches.add(new WatchRegistration(path, mode, watcher));
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
