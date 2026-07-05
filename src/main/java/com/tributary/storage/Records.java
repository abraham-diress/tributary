package com.tributary.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The record format, shared by the on-disk log and the wire protocol. Every record is
 * self-describing and checksummed, so a segment can be scanned, validated and recovered after a
 * crash without any other metadata, and followers can append a leader's bytes verbatim.
 *
 * <pre>
 *   offset     int64   absolute offset within the partition
 *   size       int32   number of bytes that follow this field
 *   crc        int32   CRC32 of everything after this field
 *   timestamp  int64   milliseconds since epoch
 *   keyLen     int32   -1 for a null key
 *   key        bytes
 *   valueLen   int32   -1 for a null value
 *   value      bytes
 * </pre>
 */
public final class Records {
    /** offset + size fields that precede every record body. */
    public static final int LOG_OVERHEAD = 12;
    /** crc + timestamp + keyLen + valueLen. */
    static final int MIN_BODY_SIZE = 4 + 8 + 4 + 4;

    private Records() {
    }

    public static int sizeInBytes(Record r) {
        return LOG_OVERHEAD + bodySize(r.key(), r.value());
    }

    private static int bodySize(byte[] key, byte[] value) {
        return MIN_BODY_SIZE + (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
    }

    /** Encodes records with consecutive offsets starting at {@code baseOffset}. */
    public static ByteBuffer encode(long baseOffset, List<Record> records, long defaultTimestamp) {
        int total = 0;
        for (Record r : records) {
            total += sizeInBytes(r);
        }
        ByteBuffer buf = ByteBuffer.allocate(total);
        long offset = baseOffset;
        for (Record r : records) {
            write(buf, offset++, r.timestamp() > 0 ? r.timestamp() : defaultTimestamp, r.key(), r.value());
        }
        buf.flip();
        return buf;
    }

    public static void write(ByteBuffer buf, long offset, long timestamp, byte[] key, byte[] value) {
        buf.putLong(offset);
        buf.putInt(bodySize(key, value));
        int crcPosition = buf.position();
        buf.putInt(0);
        int start = buf.position();
        buf.putLong(timestamp);
        putNullable(buf, key);
        putNullable(buf, value);
        buf.putInt(crcPosition, crc(buf, start, buf.position()));
    }

    /** Decodes and validates every record in the buffer without changing its position. */
    public static List<Record> decode(ByteBuffer buffer) {
        List<Record> out = new ArrayList<>();
        ByteBuffer b = buffer.duplicate();
        while (b.hasRemaining()) {
            out.add(readOne(b));
        }
        return out;
    }

    /** Reads and validates one record, advancing the buffer past it. */
    static Record readOne(ByteBuffer b) {
        if (b.remaining() < LOG_OVERHEAD) {
            throw new CorruptRecordException("Truncated record header");
        }
        long offset = b.getLong();
        int size = b.getInt();
        if (size < MIN_BODY_SIZE || size > b.remaining()) {
            throw new CorruptRecordException("Invalid record size " + size + " at offset " + offset);
        }
        int start = b.position();
        int end = start + size;
        int storedCrc = b.getInt();
        if (crc(b, start + 4, end) != storedCrc) {
            throw new CorruptRecordException("CRC mismatch for record at offset " + offset);
        }
        long timestamp = b.getLong();
        byte[] key = getNullable(b, end);
        byte[] value = getNullable(b, end);
        if (b.position() != end) {
            throw new CorruptRecordException("Record size mismatch at offset " + offset);
        }
        return new Record(offset, timestamp, key, value);
    }

    static int crc(ByteBuffer buf, int from, int to) {
        CRC32 crc = new CRC32();
        ByteBuffer d = buf.duplicate();
        d.limit(to).position(from);
        crc.update(d);
        return (int) crc.getValue();
    }

    private static void putNullable(ByteBuffer buf, byte[] bytes) {
        if (bytes == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }

    private static byte[] getNullable(ByteBuffer b, int end) {
        int len = b.getInt();
        if (len == -1) {
            return null;
        }
        if (len < 0 || b.position() + len > end) {
            throw new CorruptRecordException("Invalid field length " + len);
        }
        byte[] bytes = new byte[len];
        b.get(bytes);
        return bytes;
    }
}
