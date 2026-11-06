package com.tributary.broker;

import com.tributary.common.BrokerInfo;
import com.tributary.common.Errors;
import com.tributary.common.KafkaException;
import com.tributary.common.OffsetOutOfRangeException;
import com.tributary.common.Protocol;
import com.tributary.common.TopicPartition;
import com.tributary.storage.CorruptRecordException;
import com.tributary.storage.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Tributary broker. It serves client requests over the binary {@link Protocol}, stores the
 * partitions it hosts in local {@link Log}s, replicates follower partitions from their leaders, and
 * coordinates with other brokers through ZooKeeper:
 *
 * <pre>
 *   /brokers/ids/&lt;id&gt;                    ephemeral  broker registration (liveness)
 *   /controller                           ephemeral  id of the elected controller
 *   /topics/&lt;t&gt;                          persistent topic config
 *   /topics/&lt;t&gt;/partitions/&lt;p&gt;/state    persistent leader, leader epoch, replicas, ISR
 *   /consumers/&lt;g&gt;/offsets/&lt;t&gt;/&lt;p&gt;     persistent committed consumer offsets
 * </pre>
 *
 * ZooKeeper notifications are handled on a single "metadata" thread so state transitions for a
 * partition (leader/follower changes, truncation) never race with each other.
 */
public class TributaryBroker {
    private static final Logger LOG = LoggerFactory.getLogger(TributaryBroker.class);
    private static final Pattern STATE_PATH = Pattern.compile("^/topics/([^/]+)/partitions/(\\d+)/state$");
    private static final Pattern GROUP_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    private final BrokerConfig config;
    private final int brokerId;
    private final ZookeeperClient zkClient;
    private final Controller controller;

    /** Partitions with a replica on this broker. */
    private final Map<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();
    /** Cluster-wide partition state for every topic, mirrored from ZooKeeper. Served as metadata. */
    private final Map<String, Map<Integer, PartitionState>> clusterMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerInfo> liveBrokers = new ConcurrentHashMap<>();
    private final Map<TopicPartition, ReplicaFetcher> fetchers = new ConcurrentHashMap<>();
    private volatile int controllerId = -1;

    private final ExecutorService metadataExecutor = Executors.newSingleThreadExecutor(named("metadata-events"));
    private final ExecutorService requestPool = Executors.newCachedThreadPool(named("request-handler"));
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(named("scheduler"));
    private final Set<Socket> openConnections = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private ServerSocket serverSocket;
    private int boundPort;

    public TributaryBroker(int brokerId, String host, int port, int zkPort) {
        this(new BrokerConfig(brokerId, host, port, "localhost:" + zkPort, "data"));
    }

    public TributaryBroker(BrokerConfig config) {
        this.config = config;
        this.brokerId = config.brokerId;
        this.zkClient = new ZookeeperClient(config.zkConnect, config.zkSessionTimeoutMs);
        this.controller = new Controller(brokerId, zkClient, () -> new HashSet<>(liveBrokers.keySet()));
    }

    // ================================================================== lifecycle

    public void start() throws IOException, InterruptedException {
        if (!isRunning.compareAndSet(false, true)) {
            return;
        }
        File dataDir = brokerDataDir();
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IOException("Cannot create data directory " + dataDir);
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(config.port));
        boundPort = serverSocket.getLocalPort();

        zkClient.connect();
        zkClient.onSessionReestablished(() -> submit(this::onSessionReestablished));
        try {
            // Register watches before the initial reads so no change can slip between them.
            zkClient.addPersistentWatch("/brokers/ids", false, e -> {
                if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) submit(this::refreshLiveBrokers);
            });
            zkClient.addPersistentWatch("/controller", false, e -> submit(this::onControllerChange));
            zkClient.addPersistentWatch("/topics", true, this::onTopicsEvent);
            registerWithZookeeper();
        } catch (KeeperException e) {
            throw new IOException("ZooKeeper error during startup", e);
        }
        runOnMetadataThread(() -> {
            refreshLiveBrokers();
            loadAllTopics();
            onControllerChange();
        });

        Thread acceptor = new Thread(this::acceptConnections, "acceptor-" + brokerId);
        acceptor.start();

        scheduler.scheduleWithFixedDelay(this::shrinkIsrs, config.replicaLagMaxMs / 2, config.replicaLagMaxMs / 2, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::flushAndCheckpoint, config.flushIntervalMs, config.flushIntervalMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::applyRetention, config.retentionCheckIntervalMs, config.retentionCheckIntervalMs, TimeUnit.MILLISECONDS);

        LOG.info("Broker {} started on {}:{} (data: {})", brokerId, config.host, boundPort, dataDir.getAbsolutePath());
    }

    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }
        LOG.info("Broker {} shutting down", brokerId);
        closeQuietly(serverSocket);
        for (Socket s : openConnections) {
            closeQuietly(s);
        }
        partitions.values().forEach(Partition::wakeWaiters);
        fetchers.values().forEach(ReplicaFetcher::shutdown);
        fetchers.clear();
        // shutdown() rather than shutdownNow(): interrupting a thread mid FileChannel write closes the channel.
        scheduler.shutdown();
        requestPool.shutdown();
        metadataExecutor.shutdown();
        awaitQuietly(scheduler);
        awaitQuietly(requestPool);
        awaitQuietly(metadataExecutor);
        for (Partition p : partitions.values()) {
            p.checkpointHighWatermark();
            try {
                p.log().close();
            } catch (IOException e) {
                LOG.warn("Error closing log for {}", p.topicPartition(), e);
            }
        }
        controller.deactivate();
        zkClient.close();
        shutdownLatch.countDown();
        LOG.info("Broker {} stopped", brokerId);
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    // ================================================================== ZooKeeper / cluster state

    private void registerWithZookeeper() throws KeeperException, InterruptedException, IOException {
        String path = "/brokers/ids/" + brokerId;
        String data = new BrokerInfo(brokerId, config.host, boundPort).toJson();
        // A crashed previous incarnation's ephemeral node lingers until its session times out.
        long deadline = System.currentTimeMillis() + config.zkSessionTimeoutMs * 2L;
        while (!zkClient.createEphemeralNode(path, data)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Broker id " + brokerId + " is already registered in ZooKeeper");
            }
            LOG.info("Waiting for stale registration of broker {} to expire", brokerId);
            Thread.sleep(500);
        }
        liveBrokers.put(brokerId, new BrokerInfo(brokerId, config.host, boundPort));
        LOG.info("Registered broker {} in ZooKeeper", brokerId);
    }

    private void onSessionReestablished() throws Exception {
        registerWithZookeeper();
        refreshLiveBrokers();
        loadAllTopics();
        onControllerChange();
    }

    private void refreshLiveBrokers() throws KeeperException, InterruptedException {
        Map<Integer, BrokerInfo> current = new HashMap<>();
        for (String child : zkClient.getChildren("/brokers/ids")) {
            String data = zkClient.getData("/brokers/ids/" + child);
            if (data != null) {
                int id = Integer.parseInt(child);
                current.put(id, BrokerInfo.fromJson(id, data));
            }
        }
        liveBrokers.keySet().retainAll(current.keySet());
        liveBrokers.putAll(current);
        controller.onBrokersChanged(new HashSet<>(current.keySet()));
    }

    /** Controller election: whoever creates the ephemeral /controller node first wins. */
    private void onControllerChange() throws KeeperException, InterruptedException {
        String data = zkClient.getData("/controller");
        if (data == null && zkClient.createEphemeralNode("/controller", String.valueOf(brokerId))) {
            data = String.valueOf(brokerId);
        }
        if (data == null) {
            data = zkClient.getData("/controller");
        }
        int newController = data == null || data.isBlank() ? -1 : Integer.parseInt(data.trim());
        controllerId = newController;
        if (newController == brokerId) {
            if (!controller.isActive()) {
                controller.activate();
            }
        } else {
            controller.deactivate();
        }
    }

    private void onTopicsEvent(WatchedEvent event) {
        if (event.getPath() == null) {
            return;
        }
        Matcher m = STATE_PATH.matcher(event.getPath());
        if (m.matches() && (event.getType() == Watcher.Event.EventType.NodeCreated
                || event.getType() == Watcher.Event.EventType.NodeDataChanged)) {
            String topic = m.group(1);
            int partition = Integer.parseInt(m.group(2));
            submit(() -> refreshPartitionState(topic, partition));
        }
    }

    private void loadAllTopics() throws Exception {
        for (String topic : zkClient.getChildren("/topics")) {
            for (String p : zkClient.getChildren("/topics/" + topic + "/partitions")) {
                refreshPartitionState(topic, Integer.parseInt(p));
            }
        }
    }

    private void refreshPartitionState(String topic, int partitionId) throws Exception {
        String data = zkClient.getData(statePath(topic, partitionId));
        if (data == null || data.isEmpty()) {
            return;
        }
        PartitionState state = PartitionState.fromJson(data);
        clusterMetadata.computeIfAbsent(topic, t -> new ConcurrentHashMap<>()).put(partitionId, state);
        if (state.replicas().contains(brokerId)) {
            applyPartitionState(getOrCreatePartition(new TopicPartition(topic, partitionId)), state);
        }
    }

    /** Makes this replica a leader or follower according to the latest state from ZooKeeper. */
    private void applyPartitionState(Partition partition, PartitionState state) throws IOException {
        TopicPartition tp = partition.topicPartition();
        if (state.leaderEpoch() < partition.leaderEpoch()) {
            return; // stale notification
        }
        boolean sameEpoch = state.leaderEpoch() == partition.leaderEpoch();
        if (state.leader() == brokerId) {
            if (sameEpoch && partition.isLeader()) {
                partition.updateState(state);
                return;
            }
            stopFetcher(tp);
            partition.makeLeader(state);
        } else {
            ReplicaFetcher fetcher = fetchers.get(tp);
            boolean upToDate = sameEpoch && !partition.isLeader()
                    && (state.leader() < 0 || (fetcher != null && fetcher.leaderId() == state.leader()));
            if (upToDate) {
                partition.updateState(state);
                return;
            }
            stopFetcher(tp);
            partition.makeFollower(state);
            if (state.leader() >= 0) {
                ReplicaFetcher f = new ReplicaFetcher(partition, state.leader(), state.leaderEpoch(), liveBrokers::get, config);
                fetchers.put(tp, f);
                f.start();
            }
        }
    }

    private void stopFetcher(TopicPartition tp) {
        ReplicaFetcher f = fetchers.remove(tp);
        if (f != null) {
            f.shutdown();
        }
    }

    private Partition getOrCreatePartition(TopicPartition tp) {
        return partitions.computeIfAbsent(tp, key -> {
            try {
                File dir = new File(new File(brokerDataDir(), key.topic()), String.valueOf(key.partition()));
                return new Partition(key, brokerId, new Log(dir, config.logConfig), this::updateIsr);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to open log for " + key, e);
            }
        });
    }

    /** Leader-side ISR change, written to ZooKeeper with compare-and-set. */
    private boolean updateIsr(Partition partition, int leaderEpoch, Set<Integer> newIsr) {
        TopicPartition tp = partition.topicPartition();
        String path = statePath(tp.topic(), tp.partition());
        try {
            for (int attempt = 0; attempt < 5; attempt++) {
                ZookeeperClient.VersionedData current = zkClient.getDataWithVersion(path);
                if (current == null) {
                    return false;
                }
                PartitionState state = PartitionState.fromJson(current.data());
                if (state.leader() != brokerId || state.leaderEpoch() != leaderEpoch) {
                    return false; // leadership moved on; the controller's view wins
                }
                List<Integer> ordered = state.replicas().stream().filter(newIsr::contains).toList();
                if (zkClient.setData(path, state.withIsr(ordered).toJson(), current.version())) {
                    partition.applyIsr(leaderEpoch, newIsr);
                    return true;
                }
            }
        } catch (KeeperException | RuntimeException e) {
            LOG.warn("{}: failed to update ISR", tp, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    // ================================================================== background tasks

    private void shrinkIsrs() {
        long now = System.currentTimeMillis();
        for (Partition p : partitions.values()) {
            Set<Integer> lagging = p.laggingReplicas(now, config.replicaLagMaxMs);
            if (!lagging.isEmpty()) {
                Set<Integer> newIsr = new HashSet<>(p.inSyncReplicas());
                newIsr.removeAll(lagging);
                if (updateIsr(p, p.leaderEpoch(), newIsr)) {
                    LOG.info("{}: shrank ISR to {} (lagging: {})", p.topicPartition(), newIsr, lagging);
                }
            }
        }
    }

    private void flushAndCheckpoint() {
        for (Partition p : partitions.values()) {
            try {
                p.log().flush();
            } catch (IOException e) {
                LOG.warn("Failed to flush {}", p.topicPartition(), e);
            }
            p.checkpointHighWatermark();
        }
    }

    private void applyRetention() {
        for (Partition p : partitions.values()) {
            try {
                p.log().deleteOldSegments();
            } catch (IOException e) {
                LOG.warn("Retention failed for {}", p.topicPartition(), e);
            }
        }
    }

    // ================================================================== networking

    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                openConnections.add(socket);
                requestPool.submit(() -> handleClient(socket));
            } catch (SocketException e) {
                if (isRunning.get()) {
                    LOG.error("Error accepting connection", e);
                }
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOG.error("Error accepting connection", e);
                }
            }
        }
    }

    /** One thread per connection; requests on a connection are processed in order. */
    private void handleClient(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024))) {
            while (isRunning.get()) {
                ByteBuffer request;
                try {
                    request = Protocol.readFrame(in);
                } catch (EOFException e) {
                    return; // client closed the connection
                }
                Protocol.writeFrame(out, processClientMessage(request));
            }
        } catch (IOException e) {
            if (isRunning.get()) {
                LOG.debug("Connection closed: {}", e.getMessage());
            }
        } finally {
            openConnections.remove(socket);
        }
    }

    ByteBuffer processClientMessage(ByteBuffer buffer) {
        byte type = buffer.get();
        try {
            return switch (type) {
                case Protocol.PRODUCE -> handleProduceRequest(Protocol.decodeProduceRequest(buffer));
                case Protocol.FETCH -> handleFetchRequest(Protocol.decodeFetchRequest(buffer));
                case Protocol.METADATA -> handleMetadataRequest();
                case Protocol.CREATE_TOPIC -> handleCreateTopicRequest(Protocol.decodeCreateTopicRequest(buffer));
                case Protocol.OFFSET_COMMIT -> handleOffsetCommitRequest(Protocol.decodeOffsetCommitRequest(buffer));
                case Protocol.OFFSET_FETCH -> handleOffsetFetchRequest(Protocol.decodeOffsetFetchRequest(buffer));
                default -> Protocol.errorResponse(type, Errors.INVALID_REQUEST, "Unknown request type " + type);
            };
        } catch (KafkaException e) {
            return Protocol.errorResponse(type, e.code(), e.detail());
        } catch (CorruptRecordException e) {
            return Protocol.errorResponse(type, Errors.CORRUPT_MESSAGE, e.getMessage());
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            return Protocol.errorResponse(type, Errors.INVALID_REQUEST, "Malformed request: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Protocol.errorResponse(type, Errors.UNKNOWN_SERVER_ERROR, "Interrupted");
        } catch (Exception e) {
            LOG.error("Error handling request type {}", type, e);
            return Protocol.errorResponse(type, Errors.UNKNOWN_SERVER_ERROR, e.toString());
        }
    }

    // ================================================================== request handlers

    private ByteBuffer handleProduceRequest(Protocol.ProduceRequest req) throws IOException, InterruptedException {
        if (req.records().isEmpty()) {
            throw new KafkaException(Errors.INVALID_REQUEST, "Produce request has no records");
        }
        Partition partition = leaderPartition(req.topic(), req.partition());
        if (req.acks() == Protocol.ACKS_ALL && partition.inSyncReplicas().size() < config.minInsyncReplicas) {
            throw new KafkaException(Errors.NOT_ENOUGH_REPLICAS, "ISR " + partition.inSyncReplicas()
                    + " is smaller than min.insync.replicas=" + config.minInsyncReplicas);
        }
        long baseOffset = partition.appendAsLeader(req.records());
        long lastOffset = baseOffset + req.records().size() - 1;
        if (req.acks() == Protocol.ACKS_ALL && !partition.awaitHighWatermark(lastOffset + 1, req.timeoutMs())) {
            throw new KafkaException(Errors.REQUEST_TIMED_OUT, "Timed out waiting for offset " + lastOffset
                    + " to be replicated to the ISR " + partition.inSyncReplicas());
        }
        return Protocol.encodeProduceResponse(baseOffset);
    }

    private ByteBuffer handleFetchRequest(Protocol.FetchRequest req) throws IOException, InterruptedException {
        Partition partition = leaderPartition(req.topic(), req.partition());
        int maxWait = Math.max(0, Math.min(req.maxWaitMs(), 30_000));
        try {
            Partition.FetchData data;
            if (req.replicaId() >= 0) {
                if (req.leaderEpoch() != partition.leaderEpoch()) {
                    throw new KafkaException(Errors.FENCED_LEADER_EPOCH, "Fetch epoch " + req.leaderEpoch()
                            + " does not match leader epoch " + partition.leaderEpoch());
                }
                data = partition.readForFollower(req.replicaId(), req.offset(), req.maxBytes(), maxWait);
            } else {
                data = partition.readForConsumer(req.offset(), req.maxBytes(), maxWait);
            }
            return Protocol.encodeFetchResponse(Errors.NONE, null, data.highWatermark(), data.logStartOffset(),
                    data.logEndOffset(), data.records());
        } catch (OffsetOutOfRangeException e) {
            return Protocol.encodeFetchResponse(Errors.OFFSET_OUT_OF_RANGE, e.detail(), partition.highWatermark(),
                    e.logStartOffset(), e.logEndOffset(), ByteBuffer.allocate(0));
        }
    }

    private ByteBuffer handleMetadataRequest() {
        List<Protocol.TopicMetadata> topics = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionState>> topic : clusterMetadata.entrySet()) {
            List<Protocol.PartitionMetadata> parts = new ArrayList<>();
            for (Map.Entry<Integer, PartitionState> p : topic.getValue().entrySet()) {
                PartitionState s = p.getValue();
                parts.add(new Protocol.PartitionMetadata(p.getKey(), s.leader(), s.leaderEpoch(), s.replicas(), s.isr()));
            }
            parts.sort(Comparator.comparingInt(Protocol.PartitionMetadata::id));
            topics.add(new Protocol.TopicMetadata(topic.getKey(), parts));
        }
        topics.sort(Comparator.comparing(Protocol.TopicMetadata::name));
        List<BrokerInfo> brokers = new ArrayList<>(liveBrokers.values());
        brokers.sort(Comparator.comparingInt(BrokerInfo::getId));
        return Protocol.encodeMetadataResponse(controllerId, brokers, topics);
    }

    private ByteBuffer handleCreateTopicRequest(Protocol.CreateTopicRequest req) throws KafkaException {
        if (!controller.isActive()) {
            throw new KafkaException(Errors.NOT_CONTROLLER, "The controller is broker " + controllerId);
        }
        controller.createTopic(req.topic(), req.numPartitions(), req.replicationFactor());
        return Protocol.emptyResponse(Protocol.CREATE_TOPIC);
    }

    private ByteBuffer handleOffsetCommitRequest(Protocol.OffsetCommitRequest req) throws Exception {
        String path = offsetPath(req.group(), req.topic(), req.partition());
        zkClient.createPersistentNode(path, Long.toString(req.offset()));
        return Protocol.emptyResponse(Protocol.OFFSET_COMMIT);
    }

    private ByteBuffer handleOffsetFetchRequest(Protocol.OffsetFetchRequest req) throws Exception {
        String data = zkClient.getData(offsetPath(req.group(), req.topic(), req.partition()));
        return Protocol.encodeOffsetFetchResponse(data == null || data.isBlank() ? -1 : Long.parseLong(data.trim()));
    }

    private String offsetPath(String group, String topic, int partition) throws KafkaException {
        if (group == null || !GROUP_NAME.matcher(group).matches() || topic == null || !GROUP_NAME.matcher(topic).matches()) {
            throw new KafkaException(Errors.INVALID_REQUEST, "Invalid group or topic name");
        }
        return "/consumers/" + group + "/offsets/" + topic + "/" + partition;
    }

    private Partition leaderPartition(String topic, int partitionId) throws KafkaException {
        Partition partition = partitions.get(new TopicPartition(topic, partitionId));
        if (partition == null) {
            Map<Integer, PartitionState> known = clusterMetadata.get(topic);
            if (known != null && known.containsKey(partitionId)) {
                throw new KafkaException(Errors.NOT_LEADER, "Broker " + brokerId + " is not the leader for "
                        + topic + "-" + partitionId);
            }
            throw new KafkaException(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic + "-" + partitionId);
        }
        if (!partition.isLeader()) {
            throw new KafkaException(Errors.NOT_LEADER, "Broker " + brokerId + " is not the leader for "
                    + topic + "-" + partitionId + " (leader is " + partition.leaderId() + ")");
        }
        return partition;
    }

    // ================================================================== utilities

    @FunctionalInterface
    private interface MetadataTask {
        void run() throws Exception;
    }

    private void submit(MetadataTask task) {
        try {
            metadataExecutor.submit(() -> {
                try {
                    task.run();
                } catch (Exception e) {
                    if (isRunning.get()) {
                        LOG.error("Error processing cluster metadata change", e);
                    }
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // shutting down
        }
    }

    private void runOnMetadataThread(MetadataTask task) throws IOException, InterruptedException {
        Future<?> f = metadataExecutor.submit(() -> {
            task.run();
            return null;
        });
        try {
            f.get();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IOException("Broker initialization failed", e.getCause());
        }
    }

    private static String statePath(String topic, int partition) {
        return "/topics/" + topic + "/partitions/" + partition + "/state";
    }

    private File brokerDataDir() {
        return new File(config.dataDir, String.valueOf(brokerId));
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private static void closeQuietly(java.io.Closeable c) {
        try {
            if (c != null) c.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    private static void awaitQuietly(ExecutorService executor) {
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ================================================================== accessors (used by tests and tools)

    public int getBrokerId() {
        return brokerId;
    }

    public int getPort() {
        return boundPort;
    }

    public boolean isController() {
        return controller.isActive();
    }

    public Partition getPartition(String topic, int partition) {
        return partitions.get(new TopicPartition(topic, partition));
    }

    // ================================================================== main

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TributaryBroker <brokerId> <host> <port> <zkPort|zkHost:zkPort> [dataDir]");
            System.exit(1);
        }
        int brokerId = Integer.parseInt(args[0]);
        String zk = args[3].contains(":") ? args[3] : "localhost:" + args[3];
        String dataDir = args.length > 4 ? args[4] : "data";
        TributaryBroker broker = new TributaryBroker(new BrokerConfig(brokerId, args[1], Integer.parseInt(args[2]), zk, dataDir));
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop, "shutdown-hook"));
        broker.start();
        broker.awaitShutdown();
    }
}
