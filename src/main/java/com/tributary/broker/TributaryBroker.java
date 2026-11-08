package com.tributary.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Tributary broker. It stores partitions on disk, serves produce and fetch requests,
 * replicates messages to follower brokers, and coordinates with the rest of the cluster
 * through ZooKeeper. One broker is elected controller: it creates topics and picks new
 * partition leaders when a broker goes down.
 */
public class TributaryBroker {
    private static final Logger LOGGER = Logger.getLogger(TributaryBroker.class.getName());
    private static final String DATA_DIR = "data";
    private static final int MAX_FETCH_BYTES = 1024 * 1024;
    private static final Pattern LEADER = Pattern.compile("\"leader\":(-?\\d+)");
    private static final Pattern REPLICAS = Pattern.compile("\"replicas\":\\[([^\\]]*)\\]");
    private static final Pattern PARTITIONS = Pattern.compile("\"partitions\":(\\d+)");
    private static final Pattern HOST = Pattern.compile("\"host\":\"([^\"]+)\"");
    private static final Pattern PORT = Pattern.compile("\"port\":(\\d+)");

    private final int brokerId;
    private final String brokerHost;
    private int brokerPort;
    private final String dataDir;
    private final ZookeeperClient zkClient;

    /** Partitions this broker stores (it is the leader or a follower). */
    private final Map<String, Map<Integer, Partition>> topics = new ConcurrentHashMap<>();
    /** Leader and replicas of every partition in the cluster, used to route requests. */
    private final Map<String, Map<Integer, Protocol.PartitionMetadata>> topicMetadata = new ConcurrentHashMap<>();
    /** Live brokers. */
    private final Map<Integer, BrokerInfo> clusterMetadata = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isController = new AtomicBoolean(false);
    private ServerSocketChannel serverChannel;

    public TributaryBroker(int brokerId, String host, int port, int zkPort) {
        this(brokerId, host, port, "localhost:" + zkPort, DATA_DIR);
    }

    public TributaryBroker(int brokerId, String host, int port, String zkConnect, String dataDir) {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.dataDir = dataDir + File.separator + brokerId;
        this.zkClient = new ZookeeperClient(zkConnect);
    }

    // ================================================================ start / stop

    public void start() throws IOException {
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(brokerPort));
        brokerPort = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        isRunning.set(true);

        registerWithZookeeper();
        loadExistingTopics();
        electController();

        Thread acceptor = new Thread(this::acceptConnections, "broker-" + brokerId + "-acceptor");
        acceptor.start();
        LOGGER.info("Broker " + brokerId + " started on " + brokerHost + ":" + brokerPort);
    }

    public void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }
        try {
            serverChannel.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing server socket", e);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Map<Integer, Partition> partitions : topics.values()) {
            for (Partition partition : partitions.values()) {
                try {
                    partition.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Error closing partition", e);
                }
            }
        }
        zkClient.close();
        LOGGER.info("Broker " + brokerId + " stopped");
    }

    // ================================================================ ZooKeeper

    private void registerWithZookeeper() throws IOException {
        try {
            zkClient.connect();
            String path = "/brokers/ids/" + brokerId;
            String data = "{\"host\":\"" + brokerHost + "\",\"port\":" + brokerPort + "}";
            // After a crash, the old registration stays until its ZooKeeper session times out
            long deadline = System.currentTimeMillis() + 20_000;
            while (!zkClient.createEphemeralNode(path, data)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException("Broker id " + brokerId + " is already registered");
                }
                Thread.sleep(500);
            }
            clusterMetadata.put(brokerId, new BrokerInfo(brokerId, brokerHost, brokerPort));
            zkClient.watchChildren("/brokers/ids", this::onBrokersChanged);
            LOGGER.info("Broker " + brokerId + " registered with ZooKeeper");
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to register with ZooKeeper", e);
        }
    }

    /** Called whenever a broker joins or leaves the cluster. */
    private synchronized void onBrokersChanged(List<String> brokerIds) {
        Set<Integer> live = new HashSet<>();
        for (String id : brokerIds) {
            int brokerId = Integer.parseInt(id);
            live.add(brokerId);
            if (!clusterMetadata.containsKey(brokerId)) {
                try {
                    String data = zkClient.getData("/brokers/ids/" + id);
                    if (data != null) {
                        clusterMetadata.put(brokerId, parseBrokerInfo(brokerId, data));
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Could not read broker " + id, e);
                }
            }
        }
        clusterMetadata.keySet().retainAll(live);
        LOGGER.info("Broker " + brokerId + " sees live brokers " + live);

        if (isController.get()) {
            rebalancePartitions();
        }
    }

    /** The first broker to create the ephemeral /controller node becomes the controller. */
    private void electController() {
        try {
            if (zkClient.createEphemeralNode("/controller", String.valueOf(brokerId))) {
                isController.set(true);
                LOGGER.info("Broker " + brokerId + " is now the controller");
                rebalancePartitions();
            } else {
                isController.set(false);
            }
            zkClient.watchNode("/controller", this::onControllerChange);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed, retrying", e);
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    if (isRunning.get()) {
                        electController();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    private void onControllerChange() {
        if (isRunning.get()) {
            electController();
        }
    }

    private int getControllerId() throws Exception {
        String data = zkClient.getData("/controller");
        return data == null || data.isEmpty() ? -1 : Integer.parseInt(data.trim());
    }

    // ================================================================ topics

    private void loadExistingTopics() throws IOException {
        try {
            for (String topic : zkClient.getChildren("/topics")) {
                loadTopic(topic);
            }
        } catch (Exception e) {
            throw new IOException("Failed to load topics", e);
        }
    }

    /** Reads a topic's partition assignments from ZooKeeper and opens the partitions stored here. */
    private synchronized void loadTopic(String topic) throws Exception {
        String config = zkClient.getData("/topics/" + topic);
        if (config == null) {
            return;
        }
        Matcher m = PARTITIONS.matcher(config);
        if (!m.find()) {
            return;
        }
        int numPartitions = Integer.parseInt(m.group(1));

        Map<Integer, Protocol.PartitionMetadata> metadata = topicMetadata.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
        for (int p = 0; p < numPartitions; p++) {
            String state = zkClient.getData(statePath(topic, p));
            if (state == null) {
                continue;
            }
            int leader = parseLeader(state);
            List<Integer> replicas = parseReplicas(state);
            metadata.put(p, new Protocol.PartitionMetadata(p, leader, replicas));

            if (replicas.contains(brokerId)) {
                List<Integer> followers = new ArrayList<>(replicas);
                followers.remove(Integer.valueOf(leader));
                Map<Integer, Partition> partitions = topics.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
                Partition partition = partitions.get(p);
                if (partition == null) {
                    String dir = dataDir + File.separator + topic + File.separator + p;
                    partitions.put(p, new Partition(p, leader, followers, dir));
                } else {
                    partition.setLeader(leader);
                    partition.setFollowers(followers);
                }
            }
        }
    }

    /** Controller only: assigns replicas round-robin across live brokers and saves them in ZooKeeper. */
    private void createTopic(String topic, int numPartitions, short replicationFactor) throws Exception {
        if (zkClient.exists("/topics/" + topic)) {
            throw new IllegalArgumentException("Topic " + topic + " already exists");
        }
        List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
        Collections.sort(brokers);
        if (replicationFactor < 1 || replicationFactor > brokers.size()) {
            throw new IllegalArgumentException("Replication factor " + replicationFactor
                    + " must be between 1 and the number of live brokers (" + brokers.size() + ")");
        }

        for (int p = 0; p < numPartitions; p++) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < replicationFactor; i++) {
                replicas.add(brokers.get((p + i) % brokers.size()));
            }
            zkClient.createPersistentNode(statePath(topic, p), stateJson(replicas.get(0), replicas));
        }
        zkClient.createPersistentNode("/topics/" + topic,
                "{\"partitions\":" + numPartitions + ",\"replication_factor\":" + replicationFactor + "}");

        loadTopic(topic);
        notifyBrokers(topic);
        LOGGER.info("Created topic " + topic + " with " + numPartitions + " partitions, replication factor " + replicationFactor);
    }

    /**
     * Controller only: gives every partition whose leader is down a new leader, chosen from its
     * live replicas, then tells the other brokers to reload the topic.
     */
    private synchronized void rebalancePartitions() {
        if (!isController.get()) {
            return;
        }
        try {
            Set<Integer> live = new HashSet<>(clusterMetadata.keySet());
            for (String topic : zkClient.getChildren("/topics")) {
                boolean changed = false;
                for (String p : zkClient.getChildren("/topics/" + topic + "/partitions")) {
                    int partition = Integer.parseInt(p);
                    String state = zkClient.getData(statePath(topic, partition));
                    if (state == null) {
                        continue;
                    }
                    int leader = parseLeader(state);
                    if (live.contains(leader)) {
                        continue;
                    }
                    List<Integer> replicas = parseReplicas(state);
                    int newLeader = -1;
                    for (int replica : replicas) {
                        if (live.contains(replica)) {
                            newLeader = replica;
                            break;
                        }
                    }
                    if (newLeader != leader) {
                        zkClient.createPersistentNode(statePath(topic, partition), stateJson(newLeader, replicas));
                        LOGGER.info("Partition " + topic + "-" + partition + " leader changed from " + leader + " to " + newLeader);
                        changed = true;
                    }
                }
                if (changed) {
                    loadTopic(topic);
                    notifyBrokers(topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to rebalance partitions", e);
        }
    }

    /** Tells every other live broker to reload a topic from ZooKeeper. */
    private void notifyBrokers(String topic) {
        for (BrokerInfo broker : clusterMetadata.values()) {
            if (broker.getId() == brokerId) {
                continue;
            }
            try {
                sendRequest(broker, Protocol.encodeTopicNotification(topic));
            } catch (IOException e) {
                LOGGER.warning("Could not notify broker " + broker.getId() + " about topic " + topic);
            }
        }
    }

    // ================================================================ networking

    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                executor.submit(() -> handleClient(clientChannel));
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /** Each connection gets its own thread, which handles its requests one at a time. */
    private void handleClient(SocketChannel clientChannel) {
        try (clientChannel) {
            while (isRunning.get()) {
                ByteBuffer request = Protocol.receive(clientChannel);
                processClientMessage(clientChannel, request);
            }
        } catch (IOException e) {
            // client disconnected
        }
    }

    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        try {
            switch (messageType) {
                case Protocol.PRODUCE:
                    handleProduceRequest(clientChannel, buffer);
                    break;
                case Protocol.FETCH:
                    handleFetchRequest(clientChannel, buffer);
                    break;
                case Protocol.METADATA:
                    handleMetadataRequest(clientChannel);
                    break;
                case Protocol.CREATE_TOPIC:
                    handleCreateTopicRequest(clientChannel, buffer);
                    break;
                case Protocol.REPLICATE:
                    handleReplicateRequest(clientChannel, buffer);
                    break;
                case Protocol.TOPIC_NOTIFICATION:
                    handleTopicNotification(clientChannel, buffer);
                    break;
                default:
                    Protocol.sendErrorResponse(clientChannel, "Unknown message type: " + messageType);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    /** Opens a connection to another broker, sends one request and returns the response. */
    private ByteBuffer sendRequest(BrokerInfo broker, ByteBuffer request) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(broker.getHost(), broker.getPort()))) {
            Protocol.send(channel, request);
            return Protocol.receive(channel);
        }
    }

    // ================================================================ request handlers

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws Exception {
        String topic = Protocol.getString(buffer);
        int partitionId = buffer.getInt();
        byte[] message = new byte[buffer.getInt()];
        buffer.get(message);

        Protocol.PartitionMetadata metadata = getPartitionMetadata(topic, partitionId);
        if (metadata.getLeader() != brokerId) {
            // Forward to the leader
            BrokerInfo leader = clusterMetadata.get(metadata.getLeader());
            if (leader == null) {
                throw new IllegalStateException("No leader available for " + topic + "-" + partitionId);
            }
            Protocol.send(clientChannel, sendRequest(leader, Protocol.encodeProduceRequest(topic, partitionId, message)));
            return;
        }

        Partition partition = topics.get(topic).get(partitionId);
        long offset = partition.append(message);
        replicateToFollowers(topic, partition, message, offset);
        Protocol.send(clientChannel, Protocol.encodeProduceResponse(offset));
    }

    /**
     * Sends the message to every live follower in parallel and waits for their acknowledgements
     * before the producer is answered. A follower that missed earlier messages (for example,
     * because it was restarted) is sent those first.
     */
    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        List<Future<?>> pending = new ArrayList<>();
        for (int followerId : partition.getFollowers()) {
            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) {
                continue;
            }
            pending.add(executor.submit(() -> {
                try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(follower.getHost(), follower.getPort()))) {
                    long followerNext = replicate(channel, topic, partition.getId(), offset, message);
                    while (followerNext <= offset) {
                        List<byte[]> missing = partition.readMessages(followerNext, MAX_FETCH_BYTES);
                        if (missing.isEmpty()) {
                            break;
                        }
                        long sendOffset = followerNext;
                        for (byte[] m : missing) {
                            followerNext = replicate(channel, topic, partition.getId(), sendOffset++, m);
                        }
                    }
                } catch (IOException e) {
                    LOGGER.warning("Replication of " + topic + "-" + partition.getId() + " to broker " + followerId + " failed: " + e.getMessage());
                }
                return null;
            }));
        }
        for (Future<?> f : pending) {
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.warning("Timed out waiting for replication of " + topic + "-" + partition.getId());
            }
        }
    }

    /** Returns the follower's next offset after it processed the message. */
    private long replicate(SocketChannel channel, String topic, int partitionId, long offset, byte[] message) throws IOException {
        Protocol.send(channel, Protocol.encodeReplicateRequest(topic, partitionId, offset, message));
        ByteBuffer ack = Protocol.receive(channel);
        if (ack.get() != Protocol.REPLICATE_ACK) {
            throw new IOException("Follower rejected replication: " + Protocol.getString(ack));
        }
        return ack.getLong();
    }

    /** Follower side: appends the message only if it is the next one this replica expects. */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws Exception {
        String topic = Protocol.getString(buffer);
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        byte[] message = new byte[buffer.getInt()];
        buffer.get(message);

        Map<Integer, Partition> partitions = topics.get(topic);
        if (partitions == null || !partitions.containsKey(partitionId)) {
            loadTopic(topic);
            partitions = topics.get(topic);
        }
        if (partitions == null || !partitions.containsKey(partitionId)) {
            throw new IllegalStateException("Broker " + brokerId + " is not a replica of " + topic + "-" + partitionId);
        }
        Partition partition = partitions.get(partitionId);
        synchronized (partition) {
            if (offset == partition.getNextOffset()) {
                partition.append(message);
            }
            Protocol.send(clientChannel, Protocol.encodeReplicateAck(partition.getNextOffset()));
        }
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws Exception {
        String topic = Protocol.getString(buffer);
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        Protocol.PartitionMetadata metadata = getPartitionMetadata(topic, partitionId);
        if (metadata.getLeader() != brokerId) {
            BrokerInfo leader = clusterMetadata.get(metadata.getLeader());
            if (leader == null) {
                throw new IllegalStateException("No leader available for " + topic + "-" + partitionId);
            }
            Protocol.send(clientChannel, sendRequest(leader, Protocol.encodeFetchRequest(topic, partitionId, offset, maxBytes)));
            return;
        }

        Partition partition = topics.get(topic).get(partitionId);
        List<byte[]> messages = partition.readMessages(offset, Math.min(maxBytes, MAX_FETCH_BYTES));
        Protocol.send(clientChannel, Protocol.encodeFetchResponse(offset, messages));
    }

    private void handleMetadataRequest(SocketChannel clientChannel) throws IOException {
        List<BrokerInfo> brokers = new ArrayList<>(clusterMetadata.values());
        brokers.sort((a, b) -> Integer.compare(a.getId(), b.getId()));
        List<Protocol.TopicMetadata> result = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Protocol.PartitionMetadata>> entry : topicMetadata.entrySet()) {
            List<Protocol.PartitionMetadata> partitions = new ArrayList<>(entry.getValue().values());
            partitions.sort((a, b) -> Integer.compare(a.getId(), b.getId()));
            result.add(new Protocol.TopicMetadata(entry.getKey(), partitions));
        }
        Protocol.send(clientChannel, Protocol.encodeMetadataResponse(brokers, result));
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws Exception {
        String topic = Protocol.getString(buffer);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if (!isController.get()) {
            // Only the controller creates topics, so forward the request
            BrokerInfo controller = clusterMetadata.get(getControllerId());
            if (controller == null) {
                throw new IllegalStateException("No controller available");
            }
            Protocol.send(clientChannel, sendRequest(controller,
                    Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor)));
            return;
        }
        createTopic(topic, numPartitions, replicationFactor);
        Protocol.send(clientChannel, Protocol.encodeCreateTopicResponse());
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws Exception {
        loadTopic(Protocol.getString(buffer));
        Protocol.send(clientChannel, Protocol.encodeTopicNotificationAck());
    }

    // ================================================================ helpers

    private Protocol.PartitionMetadata getPartitionMetadata(String topic, int partitionId) throws Exception {
        Map<Integer, Protocol.PartitionMetadata> partitions = topicMetadata.get(topic);
        if (partitions == null) {
            loadTopic(topic);
            partitions = topicMetadata.get(topic);
        }
        if (partitions == null || !partitions.containsKey(partitionId)) {
            throw new IllegalArgumentException("Unknown topic or partition: " + topic + "-" + partitionId);
        }
        return partitions.get(partitionId);
    }

    private static String statePath(String topic, int partition) {
        return "/topics/" + topic + "/partitions/" + partition + "/state";
    }

    private static String stateJson(int leader, List<Integer> replicas) {
        return "{\"leader\":" + leader + ",\"replicas\":" + replicas.toString().replace(" ", "") + "}";
    }

    private static int parseLeader(String json) {
        Matcher m = LEADER.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    private static List<Integer> parseReplicas(String json) {
        List<Integer> replicas = new ArrayList<>();
        Matcher m = REPLICAS.matcher(json);
        if (m.find()) {
            for (String s : m.group(1).split(",")) {
                if (!s.isBlank()) {
                    replicas.add(Integer.parseInt(s.trim()));
                }
            }
        }
        return replicas;
    }

    private static BrokerInfo parseBrokerInfo(int id, String json) {
        Matcher host = HOST.matcher(json);
        Matcher port = PORT.matcher(json);
        if (!host.find() || !port.find()) {
            throw new IllegalArgumentException("Invalid broker data: " + json);
        }
        return new BrokerInfo(id, host.group(1), Integer.parseInt(port.group(1)));
    }

    // ================================================================ accessors

    public int getBrokerId() {
        return brokerId;
    }

    public int getPort() {
        return brokerPort;
    }

    public boolean isController() {
        return isController.get();
    }

    public Partition getPartition(String topic, int partition) {
        Map<Integer, Partition> partitions = topics.get(topic);
        return partitions == null ? null : partitions.get(partition);
    }

    // ================================================================ main

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TributaryBroker <brokerId> <host> <port> <zkPort> [dataDir]");
            System.exit(1);
        }
        int brokerId = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String zk = args[3].contains(":") ? args[3] : "localhost:" + args[3];
        String dataDir = args.length > 4 ? args[4] : DATA_DIR;

        TributaryBroker broker = new TributaryBroker(brokerId, host, port, zk, dataDir);
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
        broker.start();
    }
}
