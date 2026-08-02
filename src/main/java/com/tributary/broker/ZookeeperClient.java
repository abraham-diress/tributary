package com.tributary.broker;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Connects the broker to ZooKeeper, which stores cluster metadata:
 *
 *   /brokers/ids/[id]                       ephemeral node per live broker
 *   /controller                             ephemeral node holding the controller's broker id
 *   /topics/[topic]                         topic configuration
 *   /topics/[topic]/partitions/[p]/state    leader and replicas of each partition
 */
public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 6000;

    private final String connectString;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }

    public ZookeeperClient(String host, int port) {
        this(host + ":" + port);
    }

    public ZookeeperClient(String connectString) {
        this.connectString = connectString;
    }

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
        if (!connectedSignal.await(15, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to ZooKeeper at " + connectString);
        }
        try {
            createPath("/brokers/ids");
            createPath("/topics");
        } catch (KeeperException e) {
            throw new IOException("Failed to create base paths", e);
        }
    }

    /** Creates a persistent node and any missing parents. */
    public void createPath(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) != null) {
            return;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash > 0) {
            createPath(path.substring(0, lastSlash));
        }
        try {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignored) {
            // created by another broker in the meantime
        }
    }

    /** Creates a persistent node, or updates its data if it already exists. */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash > 0) {
                createPath(path.substring(0, lastSlash));
            }
            zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.fine("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(StandardCharsets.UTF_8), -1);
            LOGGER.fine("Updated persistent node: " + path);
        }
    }

    /**
     * Creates an ephemeral node, which disappears when this broker's session ends.
     * Returns false if the node already exists.
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        try {
            zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.fine("Created ephemeral node: " + path);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, false) != null;
    }

    /** Returns the node's data, or null if the node doesn't exist. */
    public String getData(String path) throws KeeperException, InterruptedException {
        try {
            byte[] data = zooKeeper.getData(path, false, null);
            return data == null ? "" : new String(data, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException.NoNodeException ignored) {
            // already gone
        }
    }

    /**
     * Calls the callback with the current children, and again every time they change.
     * ZooKeeper watches fire only once, so the watch is re-registered after each change.
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback);
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /** Calls the callback once when the node is created, deleted or changed. */
    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Event.EventType.NodeDeleted
                        || event.getType() == Event.EventType.NodeDataChanged
                        || event.getType() == Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                zooKeeper.close();
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
                connectedSignal.await(15, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

