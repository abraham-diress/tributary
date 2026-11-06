package com.tributary.broker;

import com.tributary.common.Errors;
import com.tributary.common.KafkaException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Cluster controller logic. Exactly one broker (whoever holds the ephemeral {@code /controller}
 * node) runs it. The controller:
 * <ul>
 *   <li>creates topics and assigns replicas round-robin across live brokers</li>
 *   <li>reacts to brokers dying or rejoining by electing new partition leaders from the ISR</li>
 * </ul>
 * All writes to partition state are compare-and-set on the ZooKeeper node version, because
 * partition leaders also update the same node when they shrink or expand the ISR.
 */
final class Controller {
    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private static final Pattern VALID_TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    private final int brokerId;
    private final ZookeeperClient zk;
    private final Supplier<Set<Integer>> liveBrokers;
    private volatile boolean active;

    Controller(int brokerId, ZookeeperClient zk, Supplier<Set<Integer>> liveBrokers) {
        this.brokerId = brokerId;
        this.zk = zk;
        this.liveBrokers = liveBrokers;
    }

    boolean isActive() {
        return active;
    }

    synchronized void activate() throws KeeperException, InterruptedException {
        active = true;
        LOG.info("Broker {} is now the controller", brokerId);
        reconcileAll(liveBrokers.get());
    }

    synchronized void deactivate() {
        if (active) {
            LOG.info("Broker {} is no longer the controller", brokerId);
        }
        active = false;
    }

    synchronized void onBrokersChanged(Set<Integer> live) throws KeeperException, InterruptedException {
        if (active) {
            LOG.info("Controller: live brokers are now {}", live);
            reconcileAll(live);
        }
    }

    void createTopic(String topic, int numPartitions, int replicationFactor) throws KafkaException {
        if (!active) {
            throw new KafkaException(Errors.NOT_CONTROLLER, "Broker " + brokerId + " is not the controller");
        }
        if (topic == null || !VALID_TOPIC.matcher(topic).matches()) {
            throw new KafkaException(Errors.INVALID_TOPIC, "Topic names must match " + VALID_TOPIC.pattern());
        }
        if (numPartitions < 1 || numPartitions > 10_000) {
            throw new KafkaException(Errors.INVALID_REQUEST, "Invalid partition count " + numPartitions);
        }
        List<Integer> brokers = new ArrayList<>(liveBrokers.get());
        brokers.sort(null);
        if (replicationFactor < 1 || replicationFactor > brokers.size()) {
            throw new KafkaException(Errors.INVALID_REPLICATION_FACTOR, "Replication factor " + replicationFactor
                    + " must be between 1 and the number of live brokers (" + brokers.size() + ")");
        }

        String topicPath = "/topics/" + topic;
        List<Op> ops = new ArrayList<>();
        ops.add(create(topicPath, "{\"partitions\":" + numPartitions + ",\"replication_factor\":" + replicationFactor + "}"));
        ops.add(create(topicPath + "/partitions", ""));
        int start = Math.floorMod(topic.hashCode(), brokers.size());
        for (int p = 0; p < numPartitions; p++) {
            List<Integer> replicas = assignReplicas(brokers, start, p, replicationFactor);
            PartitionState state = new PartitionState(replicas.get(0), 0, replicas, replicas);
            ops.add(create(topicPath + "/partitions/" + p, ""));
            ops.add(create(topicPath + "/partitions/" + p + "/state", state.toJson()));
        }
        try {
            zk.multi(ops);
        } catch (KeeperException.NodeExistsException e) {
            throw new KafkaException(Errors.TOPIC_ALREADY_EXISTS, "Topic '" + topic + "' already exists");
        } catch (KeeperException e) {
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "ZooKeeper error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException(Errors.UNKNOWN_SERVER_ERROR, "Interrupted");
        }
        LOG.info("Created topic '{}' with {} partition(s), replication factor {}", topic, numPartitions, replicationFactor);
    }

    /**
     * Partition p's replicas are consecutive brokers starting at (start + p), so leaders (the first
     * replica) are spread evenly and each broker follows a mix of partitions.
     */
    static List<Integer> assignReplicas(List<Integer> sortedBrokers, int start, int partition, int replicationFactor) {
        List<Integer> replicas = new ArrayList<>(replicationFactor);
        for (int i = 0; i < replicationFactor; i++) {
            replicas.add(sortedBrokers.get((start + partition + i) % sortedBrokers.size()));
        }
        return replicas;
    }

    private static Op create(String path, String data) {
        return Op.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void reconcileAll(Set<Integer> live) throws KeeperException, InterruptedException {
        for (String topic : zk.getChildren("/topics")) {
            for (String p : zk.getChildren("/topics/" + topic + "/partitions")) {
                reconcile("/topics/" + topic + "/partitions/" + p + "/state", topic + "-" + p, live);
            }
        }
    }

    private void reconcile(String path, String name, Set<Integer> live) throws KeeperException, InterruptedException {
        for (int attempt = 0; attempt < 10 && active; attempt++) {
            ZookeeperClient.VersionedData current = zk.getDataWithVersion(path);
            if (current == null || current.data().isEmpty()) {
                return;
            }
            PartitionState state = PartitionState.fromJson(current.data());
            PartitionState next = nextState(state, live);
            if (next.equals(state)) {
                return;
            }
            if (zk.setData(path, next.toJson(), current.version())) {
                if (next.leader() != state.leader()) {
                    LOG.info("Controller: {} leader {} -> {} (epoch {}, ISR {})", name, state.leader(), next.leader(),
                            next.leaderEpoch(), next.isr());
                } else {
                    LOG.info("Controller: {} ISR {} -> {}", name, state.isr(), next.isr());
                }
                return;
            }
        }
    }

    /**
     * Pure leader-election rule. If the leader is alive, just drop dead brokers from the ISR.
     * Otherwise pick the first live ISR member in preferred-replica order and bump the epoch. If no
     * ISR member is alive, the partition goes offline (leader -1) and the ISR is kept so one of
     * those replicas can resume when it returns; we never elect an out-of-sync replica, since it
     * could be missing committed data (no "unclean" election).
     */
    static PartitionState nextState(PartitionState state, Set<Integer> live) {
        boolean leaderAlive = state.leader() >= 0 && live.contains(state.leader());
        if (leaderAlive) {
            List<Integer> isr = state.isr().stream().filter(live::contains).toList();
            return isr.equals(state.isr()) ? state : state.withIsr(isr);
        }
        Optional<Integer> newLeader = state.replicas().stream()
                .filter(r -> state.isr().contains(r) && live.contains(r))
                .findFirst();
        if (newLeader.isPresent()) {
            List<Integer> isr = state.isr().stream().filter(live::contains).toList();
            return new PartitionState(newLeader.get(), state.leaderEpoch() + 1, state.replicas(), isr);
        }
        if (state.leader() == -1) {
            return state;
        }
        return new PartitionState(-1, state.leaderEpoch() + 1, state.replicas(), state.isr());
    }
}
