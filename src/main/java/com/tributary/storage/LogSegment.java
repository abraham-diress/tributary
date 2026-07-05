package com.tributary.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * One append-only chunk of a partition's log: a {@code <baseOffset>.log} file of records plus a
 * {@code <baseOffset>.index} sparse index. File names are the 20-digit zero-padded base offset so
 * they sort lexicographically in offset order.
 */
final class LogSegment implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

    final long baseOffset;
    private final File logFile;
    private final FileChannel channel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;
    private volatile int size;
    private long nextOffset;
    private int bytesSinceLastIndexEntry;

    LogSegment(File dir, long baseOffset, int indexIntervalBytes, boolean recover) throws IOException {
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.logFile = new File(dir, fileName(baseOffset, ".log"));
        File indexFile = new File(dir, fileName(baseOffset, ".index"));
        boolean indexExisted = indexFile.exists();
        this.channel = FileChannel.open(logFile.toPath(), CREATE, READ, WRITE);
        this.index = new OffsetIndex(indexFile, baseOffset);
        this.size = (int) channel.size();
        this.nextOffset = baseOffset;
        if (recover || (!indexExisted && size > 0)) {
            recover();
        }
    }

    static String fileName(long baseOffset, String suffix) {
        return String.format("%020d%s", baseOffset, suffix);
    }

    /**
     * Scans the whole segment, validating each record's size and CRC, and rebuilds the index.
     * Anything after the first invalid record (a torn write from a crash) is truncated.
     *
     * @return the number of bytes truncated
     */
    int recover() throws IOException {
        index.reset();
        bytesSinceLastIndexEntry = 0;
        long fileSize = channel.size();
        int pos = 0;
        long next = baseOffset;
        ByteBuffer header = ByteBuffer.allocate(Records.LOG_OVERHEAD);
        while (pos + Records.LOG_OVERHEAD <= fileSize) {
            header.clear();
            IoUtil.readFully(channel, header, pos);
            header.flip();
            long offset = header.getLong();
            int bodySize = header.getInt();
            if (offset < next || bodySize < Records.MIN_BODY_SIZE
                    || pos + Records.LOG_OVERHEAD + (long) bodySize > fileSize) {
                break;
            }
            ByteBuffer record = ByteBuffer.allocate(Records.LOG_OVERHEAD + bodySize);
            IoUtil.readFully(channel, record, pos);
            record.flip();
            try {
                Records.readOne(record);
            } catch (CorruptRecordException e) {
                break;
            }
            if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
                index.append(offset, pos);
                bytesSinceLastIndexEntry = 0;
            }
            int recordSize = Records.LOG_OVERHEAD + bodySize;
            pos += recordSize;
            bytesSinceLastIndexEntry += recordSize;
            next = offset + 1;
        }
        int truncated = (int) (fileSize - pos);
        if (truncated > 0) {
            LOG.warn("Truncating {} invalid bytes from the end of {}", truncated, logFile);
            channel.truncate(pos);
        }
        size = pos;
        nextOffset = next;
        return truncated;
    }

    /** Appends already-encoded records whose offsets are {@code firstOffset..lastOffset}. */
    void append(long firstOffset, long lastOffset, ByteBuffer records) throws IOException {
        int position = size;
        if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
            index.append(firstOffset, position);
            bytesSinceLastIndexEntry = 0;
        }
        int n = records.remaining();
        IoUtil.writeFully(channel, records, position);
        bytesSinceLastIndexEntry += n;
        nextOffset = lastOffset + 1;
        size = position + n;
    }

    /**
     * Reads whole records starting with the first one whose offset is >= {@code startOffset}, up to
     * {@code maxBytes} and stopping before {@code maxOffsetExclusive}. If the first record alone
     * exceeds maxBytes it is still returned when {@code minOneRecord} is set, so a consumer can
     * never get stuck behind a large message.
     */
    ByteBuffer read(long startOffset, int maxBytes, long maxOffsetExclusive, boolean minOneRecord) throws IOException {
        int start = translateOffset(startOffset);
        if (start < 0) {
            return ByteBuffer.allocate(0);
        }
        int available = size - start;
        int chunk = Math.min(available, Math.max(maxBytes, 0));
        ByteBuffer buf = ByteBuffer.allocate(chunk);
        IoUtil.readFully(channel, buf, start);
        buf.flip();
        int end = 0;
        while (end + Records.LOG_OVERHEAD <= chunk) {
            long offset = buf.getLong(end);
            int recordEnd = end + Records.LOG_OVERHEAD + buf.getInt(end + 8);
            if (offset >= maxOffsetExclusive || recordEnd > chunk) {
                break;
            }
            end = recordEnd;
        }
        if (end == 0 && minOneRecord && available >= Records.LOG_OVERHEAD) {
            ByteBuffer header = ByteBuffer.allocate(Records.LOG_OVERHEAD);
            IoUtil.readFully(channel, header, start);
            header.flip();
            if (header.getLong() >= maxOffsetExclusive) {
                return ByteBuffer.allocate(0);
            }
            ByteBuffer record = ByteBuffer.allocate(Records.LOG_OVERHEAD + header.getInt());
            IoUtil.readFully(channel, record, start);
            record.flip();
            return record;
        }
        buf.limit(end);
        return buf;
    }

    /** File position of the first record with offset >= target, or -1 if there is none. */
    int translateOffset(long target) throws IOException {
        int pos = index.lookup(target);
        int limit = size;
        ByteBuffer header = ByteBuffer.allocate(Records.LOG_OVERHEAD);
        while (pos + Records.LOG_OVERHEAD <= limit) {
            header.clear();
            IoUtil.readFully(channel, header, pos);
            header.flip();
            long offset = header.getLong();
            if (offset >= target) {
                return pos;
            }
            pos += Records.LOG_OVERHEAD + header.getInt();
        }
        return -1;
    }

    /** Removes every record with offset >= {@code offset}. */
    void truncateTo(long offset) throws IOException {
        int pos = translateOffset(offset);
        if (pos < 0) {
            return;
        }
        channel.truncate(pos);
        size = pos;
        index.truncateToPosition(pos);
        nextOffset = offset;
        bytesSinceLastIndexEntry = 0;
    }

    int size() {
        return size;
    }

    long nextOffset() {
        return nextOffset;
    }

    long lastModified() {
        return logFile.lastModified();
    }

    void flush() throws IOException {
        channel.force(true);
        index.flush();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        index.close();
    }

    void delete() throws IOException {
        close();
        index.delete();
        Files.deleteIfExists(logFile.toPath());
    }

    @Override
    public String toString() {
        return "LogSegment{base=" + baseOffset + ", size=" + size + "}";
    }
}
