package com.tributary.storage;

import com.tributary.common.OffsetOutOfRangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A partition's append-only commit log, split into segments keyed by base offset.
 *
 * <p>Writes go to the last ("active") segment, which rolls once it reaches
 * {@link LogConfig#segmentBytes}. Reads find the segment with a floor lookup on the segment map
 * (O(log n)), then use that segment's sparse index. A read/write lock lets many fetches run
 * concurrently while appends and truncation are exclusive.
 *
 * <p>On startup only the active segment is fully scanned and CRC-checked; closed segments were
 * flushed when they rolled and trust their index files.
 */
public final class Log implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile long logStartOffset;
    private volatile long logEndOffset;

    public Log(File dir, LogConfig config) throws IOException {
        this.dir = dir;
        this.config = config;
        Files.createDirectories(dir.toPath());
        List<Long> baseOffsets = new ArrayList<>();
        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (files != null) {
            for (File f : files) {
                try {
                    baseOffsets.add(Long.parseLong(f.getName().substring(0, f.getName().length() - 4)));
                } catch (NumberFormatException ignored) {
                    // not a segment file
                }
            }
        }
        baseOffsets.sort(null);
        for (int i = 0; i < baseOffsets.size(); i++) {
            boolean active = i == baseOffsets.size() - 1;
            long base = baseOffsets.get(i);
            segments.put(base, new LogSegment(dir, base, config.indexIntervalBytes, active));
        }
        if (segments.isEmpty()) {
            segments.put(0L, new LogSegment(dir, 0, config.indexIntervalBytes, false));
        }
        logStartOffset = segments.firstKey();
        logEndOffset = activeSegment().nextOffset();
        if (logEndOffset > 0) {
            LOG.info("Loaded log {} with {} segment(s), offsets [{}, {})", dir, segments.size(), logStartOffset, logEndOffset);
        }
    }

    /** Appends records as the partition leader, assigning offsets. Returns the first offset. */
    public long append(List<Record> records) throws IOException {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot append an empty batch");
        }
        lock.writeLock().lock();
        try {
            long base = logEndOffset;
            ByteBuffer buf = Records.encode(base, records, System.currentTimeMillis());
            writeBatch(base, base + records.size() - 1, buf);
            return base;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends records copied from the leader. They already carry offsets, which must continue
     * exactly from this log's end. CRCs are verified before anything touches disk.
     */
    public void appendAsFollower(ByteBuffer raw) throws IOException {
        List<Record> records = Records.decode(raw);
        if (records.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            long expected = logEndOffset;
            for (Record r : records) {
                if (r.offset() != expected++) {
                    throw new IllegalStateException("Non-contiguous replicated batch: expected offset "
                            + (expected - 1) + " but got " + r.offset());
                }
            }
            writeBatch(records.get(0).offset(), records.get(records.size() - 1).offset(), raw.duplicate());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeBatch(long firstOffset, long lastOffset, ByteBuffer buf) throws IOException {
        LogSegment active = maybeRoll(buf.remaining());
        active.append(firstOffset, lastOffset, buf);
        logEndOffset = lastOffset + 1;
        if (config.flushOnEveryAppend) {
            active.flush();
        }
    }

    private LogSegment maybeRoll(int incomingBytes) throws IOException {
        LogSegment active = activeSegment();
        if (active.size() > 0 && (long) active.size() + incomingBytes > config.segmentBytes) {
            active.flush();
            active = new LogSegment(dir, logEndOffset, config.indexIntervalBytes, false);
            segments.put(logEndOffset, active);
            LOG.debug("Rolled new segment {} in {}", logEndOffset, dir);
        }
        return active;
    }

    /**
     * Reads records starting at {@code offset}, up to roughly {@code maxBytes} and never at or past
     * {@code maxOffsetExclusive} (consumers pass the high watermark here).
     */
    public ByteBuffer read(long offset, int maxBytes, long maxOffsetExclusive) throws IOException {
        lock.readLock().lock();
        try {
            long start = logStartOffset;
            long end = logEndOffset;
            if (offset < start || offset > end) {
                throw new OffsetOutOfRangeException(offset, start, end);
            }
            long limit = Math.min(maxOffsetExclusive, end);
            if (offset >= limit) {
                return ByteBuffer.allocate(0);
            }
            Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);
            while (entry != null) {
                ByteBuffer records = entry.getValue().read(offset, maxBytes, limit, true);
                if (records.hasRemaining()) {
                    return records;
                }
                entry = segments.higherEntry(entry.getKey());
            }
            return ByteBuffer.allocate(0);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Removes every record with offset >= {@code offset}. Used when a follower diverged from its leader. */
    public void truncateTo(long offset) throws IOException {
        lock.writeLock().lock();
        try {
            if (offset >= logEndOffset) {
                return;
            }
            if (offset <= logStartOffset) {
                truncateFullyAndStartAt(offset);
                return;
            }
            LOG.info("Truncating log {} from {} to {}", dir, logEndOffset, offset);
            for (Long base : new ArrayList<>(segments.descendingKeySet())) {
                if (base >= offset && base > segments.firstKey()) {
                    segments.remove(base).delete();
                }
            }
            activeSegment().truncateTo(offset);
            logEndOffset = offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Deletes all data and restarts the log at {@code offset}. */
    public void truncateFullyAndStartAt(long offset) throws IOException {
        lock.writeLock().lock();
        try {
            LOG.info("Truncating log {} fully and restarting at offset {}", dir, offset);
            for (LogSegment s : segments.values()) {
                s.delete();
            }
            segments.clear();
            segments.put(offset, new LogSegment(dir, offset, config.indexIntervalBytes, false));
            logStartOffset = offset;
            logEndOffset = offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Applies the retention policy to closed segments. Returns how many segments were deleted. */
    public int deleteOldSegments() throws IOException {
        if (config.retentionMs < 0 && config.retentionBytes < 0) {
            return 0;
        }
        lock.writeLock().lock();
        try {
            int deleted = 0;
            long now = System.currentTimeMillis();
            while (segments.size() > 1) {
                LogSegment oldest = segments.firstEntry().getValue();
                boolean expired = config.retentionMs >= 0 && now - oldest.lastModified() > config.retentionMs;
                boolean oversized = config.retentionBytes >= 0 && sizeInBytes() - oldest.size() >= config.retentionBytes;
                if (!expired && !oversized) {
                    break;
                }
                segments.pollFirstEntry();
                oldest.delete();
                logStartOffset = segments.firstKey();
                deleted++;
            }
            if (deleted > 0) {
                LOG.info("Retention deleted {} segment(s) from {}; log now starts at {}", deleted, dir, logStartOffset);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public int numberOfSegments() {
        return segments.size();
    }

    public long sizeInBytes() {
        long total = 0;
        for (LogSegment s : segments.values()) {
            total += s.size();
        }
        return total;
    }

    public File dir() {
        return dir;
    }

    public void flush() throws IOException {
        lock.readLock().lock();
        try {
            activeSegment().flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            for (LogSegment s : segments.values()) {
                s.flush();
                s.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
