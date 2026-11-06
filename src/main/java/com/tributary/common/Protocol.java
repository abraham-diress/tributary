package com.tributary.common;

import com.tributary.storage.Record;
import com.tributary.storage.Records;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The binary wire protocol spoken between clients and brokers, and between brokers for replication.
 *
 * <p>Every message is a frame: {@code int32 length} followed by that many payload bytes. A request
 * payload starts with a one-byte request type; a response payload starts with the response type
 * (request type | 0x10), an {@code int16} error code and a nullable error message. Strings are
 * {@code int16}-length-prefixed UTF-8, byte arrays are {@code int32}-length-prefixed (-1 = null).
 *
 * <p>Replication reuses FETCH: a follower sends its broker id as {@code replicaId}, which lets the
 * leader track follower progress and read past the high watermark.
 */
public final class Protocol {
    // Request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte OFFSET_COMMIT = 0x05;
    public static final byte OFFSET_FETCH = 0x06;

    // Response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte OFFSET_COMMIT_RESPONSE = 0x15;
    public static final byte OFFSET_FETCH_RESPONSE = 0x16;

    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    /** replicaId used by ordinary consumers in a FETCH request. */
    public static final int CONSUMER_REPLICA_ID = -1;

    public static final short ACKS_NONE = 0;
    public static final short ACKS_LEADER = 1;
    public static final short ACKS_ALL = -1;

    private Protocol() {
    }

    // ------------------------------------------------------------------ framing

    /** Writes one frame. Does not modify the buffer's position, so a request can be resent. */
    public static void writeFrame(DataOutputStream out, ByteBuffer payload) throws IOException {
        out.writeInt(payload.remaining());
        out.write(payload.array(), payload.arrayOffset() + payload.position(), payload.remaining());
        out.flush();
    }

    public static ByteBuffer readFrame(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 1 || length > MAX_FRAME_BYTES) {
            throw new IOException("Invalid frame length " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return ByteBuffer.wrap(payload);
    }

    // ------------------------------------------------------------------ requests

    public record ProduceRequest(String topic, int partition, short acks, int timeoutMs, List<Record> records) {
    }

    public record FetchRequest(String topic, int partition, long offset, int maxBytes, int maxWaitMs,
                               int replicaId, int leaderEpoch) {
    }

    public record CreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
    }

    public record OffsetCommitRequest(String group, String topic, int partition, long offset) {
    }

    public record OffsetFetchRequest(String group, String topic, int partition) {
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, short acks, int timeoutMs, List<Record> records) {
        Writer w = new Writer(64 + records.size() * 32)
                .putByte(PRODUCE).putString(topic).putInt(partition).putShort(acks).putInt(timeoutMs)
                .putInt(records.size());
        for (Record r : records) {
            w.putLong(r.timestamp()).putBytes(r.key()).putBytes(r.value());
        }
        return w.toBuffer();
    }

    /** Convenience form used by the simple client API: one message, leader acknowledgement. */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        return encodeProduceRequest(topic, partition, ACKS_LEADER, 30_000, List.of(Record.of(null, message)));
    }

    public static ProduceRequest decodeProduceRequest(ByteBuffer b) {
        String topic = getString(b);
        int partition = b.getInt();
        short acks = b.getShort();
        int timeoutMs = b.getInt();
        int count = b.getInt();
        if (count < 0 || count > b.remaining()) {
            throw new IllegalArgumentException("Invalid record count " + count);
        }
        List<Record> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long timestamp = b.getLong();
            byte[] key = getBytes(b);
            byte[] value = getBytes(b);
            records.add(new Record(-1, timestamp, key, value));
        }
        return new ProduceRequest(topic, partition, acks, timeoutMs, records);
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes,
                                                int maxWaitMs, int replicaId, int leaderEpoch) {
        return new Writer(64).putByte(FETCH).putString(topic).putInt(partition).putLong(offset)
                .putInt(maxBytes).putInt(maxWaitMs).putInt(replicaId).putInt(leaderEpoch).toBuffer();
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        return encodeFetchRequest(topic, partition, offset, maxBytes, 0, CONSUMER_REPLICA_ID, -1);
    }

    public static FetchRequest decodeFetchRequest(ByteBuffer b) {
        return new FetchRequest(getString(b), b.getInt(), b.getLong(), b.getInt(), b.getInt(), b.getInt(), b.getInt());
    }

    public static ByteBuffer encodeMetadataRequest() {
        return new Writer(1).putByte(METADATA).toBuffer();
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        return new Writer(64).putByte(CREATE_TOPIC).putString(topic).putInt(numPartitions)
                .putShort(replicationFactor).toBuffer();
    }

    public static CreateTopicRequest decodeCreateTopicRequest(ByteBuffer b) {
        return new CreateTopicRequest(getString(b), b.getInt(), b.getShort());
    }

    public static ByteBuffer encodeOffsetCommitRequest(String group, String topic, int partition, long offset) {
        return new Writer(64).putByte(OFFSET_COMMIT).putString(group).putString(topic).putInt(partition)
                .putLong(offset).toBuffer();
    }

    public static OffsetCommitRequest decodeOffsetCommitRequest(ByteBuffer b) {
        return new OffsetCommitRequest(getString(b), getString(b), b.getInt(), b.getLong());
    }

    public static ByteBuffer encodeOffsetFetchRequest(String group, String topic, int partition) {
        return new Writer(64).putByte(OFFSET_FETCH).putString(group).putString(topic).putInt(partition).toBuffer();
    }

    public static OffsetFetchRequest decodeOffsetFetchRequest(ByteBuffer b) {
        return new OffsetFetchRequest(getString(b), getString(b), b.getInt());
    }

    // ------------------------------------------------------------------ responses

    public record ProduceResult(long baseOffset) {
    }

    /** Records are returned in the raw log format so followers can append them byte-for-byte. */
    public record FetchResult(long highWatermark, long logStartOffset, long logEndOffset, ByteBuffer rawRecords) {
        public List<Record> records() {
            return Records.decode(rawRecords);
        }
    }

    public record PartitionMetadata(int id, int leader, int leaderEpoch, List<Integer> replicas, List<Integer> isr) {
    }

    public record TopicMetadata(String name, List<PartitionMetadata> partitions) {
        public PartitionMetadata partition(int id) {
            for (PartitionMetadata p : partitions) {
                if (p.id() == id) return p;
            }
            return null;
        }
    }

    public record MetadataResult(int controllerId, List<BrokerInfo> brokers, List<TopicMetadata> topics) {
        public BrokerInfo broker(int id) {
            for (BrokerInfo b : brokers) {
                if (b.getId() == id) return b;
            }
            return null;
        }

        public TopicMetadata topic(String name) {
            for (TopicMetadata t : topics) {
                if (t.name().equals(name)) return t;
            }
            return null;
        }
    }

    private static Writer responseHeader(byte requestType, short error, String message) {
        return new Writer(64).putByte((byte) (requestType | 0x10)).putShort(error).putString(message);
    }

    public static ByteBuffer errorResponse(byte requestType, short error, String message) {
        return responseHeader(requestType, error, message).toBuffer();
    }

    public static ByteBuffer emptyResponse(byte requestType) {
        return responseHeader(requestType, Errors.NONE, null).toBuffer();
    }

    public static ByteBuffer encodeProduceResponse(long baseOffset) {
        return responseHeader(PRODUCE, Errors.NONE, null).putLong(baseOffset).toBuffer();
    }

    public static ByteBuffer encodeFetchResponse(short error, String message, long highWatermark, long logStartOffset,
                                                 long logEndOffset, ByteBuffer records) {
        return responseHeader(FETCH, error, message).putLong(highWatermark).putLong(logStartOffset)
                .putLong(logEndOffset).putBuffer(records).toBuffer();
    }

    public static ByteBuffer encodeMetadataResponse(int controllerId, Collection<BrokerInfo> brokers, List<TopicMetadata> topics) {
        Writer w = responseHeader(METADATA, Errors.NONE, null).putInt(controllerId).putInt(brokers.size());
        for (BrokerInfo b : brokers) {
            w.putInt(b.getId()).putString(b.getHost()).putInt(b.getPort());
        }
        w.putInt(topics.size());
        for (TopicMetadata t : topics) {
            w.putString(t.name()).putInt(t.partitions().size());
            for (PartitionMetadata p : t.partitions()) {
                w.putInt(p.id()).putInt(p.leader()).putInt(p.leaderEpoch()).putIntList(p.replicas()).putIntList(p.isr());
            }
        }
        return w.toBuffer();
    }

    public static ByteBuffer encodeOffsetFetchResponse(long offset) {
        return responseHeader(OFFSET_FETCH, Errors.NONE, null).putLong(offset).toBuffer();
    }

    private record Header(short error, String message) {
    }

    private static Header readHeader(ByteBuffer b, byte requestType) throws KafkaException {
        byte type = b.get();
        if (type != (byte) (requestType | 0x10)) {
            throw new KafkaException(Errors.INVALID_REQUEST, "Unexpected response type " + type + " for request " + requestType);
        }
        return new Header(b.getShort(), getString(b));
    }

    private static void throwIfError(Header h) throws KafkaException {
        if (h.error() != Errors.NONE) {
            throw new KafkaException(h.error(), h.message());
        }
    }

    /** Checks a response that carries no body (CREATE_TOPIC, OFFSET_COMMIT). */
    public static void checkResponse(ByteBuffer b, byte requestType) throws KafkaException {
        throwIfError(readHeader(b, requestType));
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer b) throws KafkaException {
        throwIfError(readHeader(b, PRODUCE));
        return new ProduceResult(b.getLong());
    }

    public static FetchResult decodeFetchResponse(ByteBuffer b) throws KafkaException {
        Header h = readHeader(b, FETCH);
        if (h.error() == Errors.OFFSET_OUT_OF_RANGE && b.hasRemaining()) {
            b.getLong();
            long start = b.getLong();
            long end = b.getLong();
            throw new OffsetOutOfRangeException(h.message(), start, end);
        }
        throwIfError(h);
        return new FetchResult(b.getLong(), b.getLong(), b.getLong(), getBuffer(b));
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer b) throws KafkaException {
        throwIfError(readHeader(b, METADATA));
        int controllerId = b.getInt();
        int brokerCount = b.getInt();
        List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            brokers.add(new BrokerInfo(b.getInt(), getString(b), b.getInt()));
        }
        int topicCount = b.getInt();
        List<TopicMetadata> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String name = getString(b);
            int partitionCount = b.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>(partitionCount);
            for (int j = 0; j < partitionCount; j++) {
                partitions.add(new PartitionMetadata(b.getInt(), b.getInt(), b.getInt(), getIntList(b), getIntList(b)));
            }
            topics.add(new TopicMetadata(name, partitions));
        }
        return new MetadataResult(controllerId, brokers, topics);
    }

    public static long decodeOffsetFetchResponse(ByteBuffer b) throws KafkaException {
        throwIfError(readHeader(b, OFFSET_FETCH));
        return b.getLong();
    }

    // ------------------------------------------------------------------ primitives

    /** Growable big-endian buffer writer. */
    public static final class Writer {
        private ByteBuffer buf;

        public Writer(int initialCapacity) {
            buf = ByteBuffer.allocate(Math.max(16, initialCapacity));
        }

        private void ensure(int n) {
            if (buf.remaining() < n) {
                ByteBuffer bigger = ByteBuffer.allocate(Math.max(buf.capacity() * 2, buf.position() + n));
                buf.flip();
                bigger.put(buf);
                buf = bigger;
            }
        }

        public Writer putByte(byte v) {
            ensure(1);
            buf.put(v);
            return this;
        }

        public Writer putShort(short v) {
            ensure(2);
            buf.putShort(v);
            return this;
        }

        public Writer putInt(int v) {
            ensure(4);
            buf.putInt(v);
            return this;
        }

        public Writer putLong(long v) {
            ensure(8);
            buf.putLong(v);
            return this;
        }

        public Writer putString(String s) {
            if (s == null) {
                return putShort((short) -1);
            }
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("String too long");
            }
            putShort((short) bytes.length);
            ensure(bytes.length);
            buf.put(bytes);
            return this;
        }

        public Writer putBytes(byte[] bytes) {
            if (bytes == null) {
                return putInt(-1);
            }
            putInt(bytes.length);
            ensure(bytes.length);
            buf.put(bytes);
            return this;
        }

        public Writer putBuffer(ByteBuffer b) {
            ByteBuffer d = b.duplicate();
            putInt(d.remaining());
            ensure(d.remaining());
            buf.put(d);
            return this;
        }

        public Writer putIntList(List<Integer> list) {
            putInt(list.size());
            for (int v : list) {
                putInt(v);
            }
            return this;
        }

        public ByteBuffer toBuffer() {
            ByteBuffer out = buf.duplicate();
            out.flip();
            return out;
        }
    }

    public static String getString(ByteBuffer b) {
        short len = b.getShort();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        b.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] getBytes(ByteBuffer b) {
        int len = b.getInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        b.get(bytes);
        return bytes;
    }

    /** Returns a zero-copy slice of a length-prefixed byte region. */
    public static ByteBuffer getBuffer(ByteBuffer b) {
        int len = b.getInt();
        ByteBuffer slice = b.slice();
        slice.limit(len);
        b.position(b.position() + len);
        return slice;
    }

    public static List<Integer> getIntList(ByteBuffer b) {
        int n = b.getInt();
        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(b.getInt());
        }
        return list;
    }
}
