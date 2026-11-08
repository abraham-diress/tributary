package com.tributary.broker;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The binary wire protocol used between clients and brokers.
 *
 * Every message is sent as a frame: a 4-byte length followed by the payload.
 * The first byte of the payload identifies the message type.
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker-to-broker request types
    public static final byte REPLICATE = 0x05;
    public static final byte TOPIC_NOTIFICATION = 0x06;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte REPLICATE_ACK = 0x15;
    public static final byte TOPIC_NOTIFICATION_ACK = 0x16;
    public static final byte ERROR_RESPONSE = 0x7F;

    private static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    // ---------------------------------------------------------------- framing

    public static void send(SocketChannel channel, ByteBuffer payload) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(4 + payload.remaining());
        frame.putInt(payload.remaining());
        frame.put(payload.duplicate());
        frame.flip();
        while (frame.hasRemaining()) {
            channel.write(frame);
        }
    }

    public static ByteBuffer receive(SocketChannel channel) throws IOException {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        readFully(channel, sizeBuffer);
        sizeBuffer.flip();
        int size = sizeBuffer.getInt();
        if (size <= 0 || size > MAX_FRAME_SIZE) {
            throw new IOException("Invalid frame size: " + size);
        }
        ByteBuffer payload = ByteBuffer.allocate(size);
        readFully(channel, payload);
        payload.flip();
        return payload;
    }

    private static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new EOFException("Connection closed");
            }
        }
    }

    // ---------------------------------------------------------------- requests

    /**
     * Layout: type (1) | topic length (2) | topic | partition (4) | message length (4) | message
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 4 + message.length);
        buffer.put(PRODUCE);
        putString(buffer, topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Layout: type (1) | topic length (2) | topic | partition (4) | offset (8) | max bytes (4)
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4);
        buffer.put(FETCH);
        putString(buffer, topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * Layout: type (1) | topic length (2) | topic | partitions (4) | replication factor (2)
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 2);
        buffer.put(CREATE_TOPIC);
        putString(buffer, topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * Sent by a partition leader to a follower.
     * Layout: type (1) | topic length (2) | topic | partition (4) | offset (8) | message length (4) | message
     */
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length);
        buffer.put(REPLICATE);
        putString(buffer, topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /** Tells another broker to (re)load a topic's metadata from ZooKeeper. */
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        putString(buffer, topicBytes);
        buffer.flip();
        return buffer;
    }

    // ---------------------------------------------------------------- responses

    public static ByteBuffer encodeProduceResponse(long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);
        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);
        buffer.flip();
        return buffer;
    }

    /**
     * Layout: type (1) | message count (4) | for each: offset (8) | length (4) | bytes
     */
    public static ByteBuffer encodeFetchResponse(long startOffset, List<byte[]> messages) {
        int size = 1 + 4;
        for (byte[] message : messages) {
            size += 8 + 4 + message.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(FETCH_RESPONSE);
        buffer.putInt(messages.size());
        long offset = startOffset;
        for (byte[] message : messages) {
            buffer.putLong(offset++);
            buffer.putInt(message.length);
            buffer.put(message);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Layout: type (1) | broker count (4) | brokers... | topic count (4) | topics...
     */
    public static ByteBuffer encodeMetadataResponse(List<BrokerInfo> brokers, List<TopicMetadata> topics) {
        List<byte[]> hosts = new ArrayList<>();
        int size = 1 + 4;
        for (BrokerInfo broker : brokers) {
            byte[] host = broker.getHost().getBytes(StandardCharsets.UTF_8);
            hosts.add(host);
            size += 4 + 2 + host.length + 4;
        }
        size += 4;
        List<byte[]> names = new ArrayList<>();
        for (TopicMetadata topic : topics) {
            byte[] name = topic.getName().getBytes(StandardCharsets.UTF_8);
            names.add(name);
            size += 2 + name.length + 4;
            for (PartitionMetadata partition : topic.getPartitions()) {
                size += 4 + 4 + 4 + 4 * partition.getReplicas().size();
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(METADATA_RESPONSE);
        buffer.putInt(brokers.size());
        for (int i = 0; i < brokers.size(); i++) {
            buffer.putInt(brokers.get(i).getId());
            putString(buffer, hosts.get(i));
            buffer.putInt(brokers.get(i).getPort());
        }
        buffer.putInt(topics.size());
        for (int i = 0; i < topics.size(); i++) {
            putString(buffer, names.get(i));
            List<PartitionMetadata> partitions = topics.get(i).getPartitions();
            buffer.putInt(partitions.size());
            for (PartitionMetadata partition : partitions) {
                buffer.putInt(partition.getId());
                buffer.putInt(partition.getLeader());
                buffer.putInt(partition.getReplicas().size());
                for (int replica : partition.getReplicas()) {
                    buffer.putInt(replica);
                }
            }
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicResponse() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.put(CREATE_TOPIC_RESPONSE);
        buffer.put((byte) 1);
        buffer.flip();
        return buffer;
    }

    /** The follower replies with its next offset so the leader knows whether it is behind. */
    public static ByteBuffer encodeReplicateAck(long followerNextOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);
        buffer.put(REPLICATE_ACK);
        buffer.putLong(followerNextOffset);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeTopicNotificationAck() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(TOPIC_NOTIFICATION_ACK);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeErrorResponse(String errorMessage) {
        byte[] bytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + bytes.length);
        buffer.put(ERROR_RESPONSE);
        putString(buffer, bytes);
        buffer.flip();
        return buffer;
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        send(channel, encodeErrorResponse(errorMessage));
    }

    // ---------------------------------------------------------------- decoding

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            return new ProduceResult(-1, getString(buffer));
        }
        if (type != PRODUCE_RESPONSE) {
            return new ProduceResult(-1, "Unexpected response type: " + type);
        }
        return new ProduceResult(buffer.getLong(), null);
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            return new FetchResult(new ArrayList<>(), getString(buffer));
        }
        if (type != FETCH_RESPONSE) {
            return new FetchResult(new ArrayList<>(), "Unexpected response type: " + type);
        }
        int count = buffer.getInt();
        List<byte[]> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            buffer.getLong(); // offset
            byte[] message = new byte[buffer.getInt()];
            buffer.get(message);
            messages.add(message);
        }
        return new FetchResult(messages, null);
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), getString(buffer));
        }
        if (type != METADATA_RESPONSE) {
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Unexpected response type: " + type);
        }
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++) {
            int id = buffer.getInt();
            String host = getString(buffer);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, host, port));
        }
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            String name = getString(buffer);
            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 0; j < partitionCount; j++) {
                int id = buffer.getInt();
                int leader = buffer.getInt();
                int replicaCount = buffer.getInt();
                List<Integer> replicas = new ArrayList<>();
                for (int k = 0; k < replicaCount; k++) {
                    replicas.add(buffer.getInt());
                }
                partitions.add(new PartitionMetadata(id, leader, replicas));
            }
            topics.add(new TopicMetadata(name, partitions));
        }
        return new MetadataResult(brokers, topics, null);
    }

    /** Returns null on success, or the error message. */
    public static String decodeCreateTopicResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            return getString(buffer);
        }
        return type == CREATE_TOPIC_RESPONSE ? null : "Unexpected response type: " + type;
    }

    // ---------------------------------------------------------------- helpers

    private static void putString(ByteBuffer buffer, byte[] bytes) {
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    public static String getString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.getShort()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------- result classes

    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static class FetchResult {
        private final List<byte[]> messages;
        private final String error;

        public FetchResult(List<byte[]> messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public List<byte[]> getMessages() {
            return messages;
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}
