package com.tributary.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Stores the messages of one partition on disk as an append-only log split into segments.
 *
 * Each segment is a pair of files named after the offset of its first message:
 *   00000000000000000000.log    messages, each written as [4-byte length][bytes]
 *   00000000000000000000.index  one 16-byte entry per message: [8-byte offset][8-byte file position]
 *
 * A new segment starts once the active one reaches 1 MB.
 */
public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final long SEGMENT_SIZE = 1024 * 1024;
    private static final int INDEX_ENTRY_SIZE = 16;

    private final int id;
    private volatile int leader;
    private volatile List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset = new AtomicLong(0);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private RandomAccessFile activeIndexFile;
    private FileChannel activeIndexChannel;
    private final List<SegmentInfo> segments = new ArrayList<>();

    private static class SegmentInfo {
        final long baseOffset;
        final File logFile;
        final File indexFile;

        SegmentInfo(long baseOffset, File logFile, File indexFile) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }
    }

    public Partition(int id, int leader, List<Integer> followers, String baseDir) throws IOException {
        this.id = id;
        this.leader = leader;
        this.followers = new ArrayList<>(followers);
        this.baseDir = baseDir;
        initialize();
    }

    // ---------------------------------------------------------------- startup

    private void initialize() throws IOException {
        File dir = new File(baseDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Could not create directory " + baseDir);
        }

        // Load existing segments, sorted by base offset
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (logFiles != null) {
            for (File logFile : logFiles) {
                String name = logFile.getName();
                long baseOffset = Long.parseLong(name.substring(0, name.length() - 4));
                segments.add(new SegmentInfo(baseOffset, logFile, new File(dir, segmentName(baseOffset) + ".index")));
            }
        }
        segments.sort((a, b) -> Long.compare(a.baseOffset, b.baseOffset));

        if (segments.isEmpty()) {
            createNewSegment(0);
            return;
        }

        // The next offset is the last segment's base offset plus the number of messages in it
        SegmentInfo last = segments.get(segments.size() - 1);
        long messageCount = recoverSegment(last);
        nextOffset.set(last.baseOffset + messageCount);
        openSegmentForAppend(last);
        if (nextOffset.get() > 0) {
            LOGGER.info("Partition " + id + " loaded " + segments.size() + " segment(s), next offset " + nextOffset.get());
        }
    }

    /**
     * Counts the complete messages in a segment and rebuilds its index. A partly written message
     * at the end (from a crash during a write) is cut off.
     */
    private long recoverSegment(SegmentInfo segment) throws IOException {
        try (RandomAccessFile log = new RandomAccessFile(segment.logFile, "rw");
             RandomAccessFile index = new RandomAccessFile(segment.indexFile, "rw")) {
            long length = log.length();
            long position = 0;
            long count = 0;
            ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            index.setLength(0);
            while (position + 4 <= length) {
                log.seek(position);
                int size = log.readInt();
                if (size < 0 || position + 4 + size > length) {
                    break;
                }
                entry.clear();
                entry.putLong(segment.baseOffset + count).putLong(position).flip();
                index.getChannel().write(entry, count * INDEX_ENTRY_SIZE);
                position += 4 + size;
                count++;
            }
            if (position < length) {
                LOGGER.warning("Truncating " + (length - position) + " incomplete bytes from " + segment.logFile);
                log.setLength(position);
            }
            return count;
        }
    }

    // ---------------------------------------------------------------- writes

    /** Appends a message and returns the offset assigned to it. */
    public long append(byte[] message) throws IOException {
        lock.writeLock().lock();
        try {
            if (activeLogChannel.size() > 0 && activeLogChannel.size() + 4 + message.length > SEGMENT_SIZE) {
                createNewSegment(nextOffset.get());
            }

            long position = activeLogChannel.size();
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();
            while (buffer.hasRemaining()) {
                activeLogChannel.write(buffer, position + buffer.position());
            }
            activeLogChannel.force(true);

            long offset = nextOffset.get();
            updateIndex(offset, position);
            nextOffset.incrementAndGet();
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndex(long offset, long position) throws IOException {
        ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        entry.putLong(offset);
        entry.putLong(position);
        entry.flip();
        activeIndexChannel.write(entry, activeIndexChannel.size());
        activeIndexChannel.force(true);
    }

    // ---------------------------------------------------------------- reads

    /** Reads messages starting at {@code offset}, up to about {@code maxBytes}. */
    public List<byte[]> readMessages(long offset, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            List<byte[]> messages = new ArrayList<>();
            if (offset < 0 || offset >= nextOffset.get()) {
                return messages;
            }

            int segmentIndex = findSegmentForOffset(offset);
            long currentOffset = offset;
            int bytesRead = 0;

            while (segmentIndex < segments.size() && currentOffset < nextOffset.get()) {
                SegmentInfo segment = segments.get(segmentIndex);
                long segmentEnd = segmentIndex + 1 < segments.size() ? segments.get(segmentIndex + 1).baseOffset : nextOffset.get();
                try (RandomAccessFile log = new RandomAccessFile(segment.logFile, "r")) {
                    log.seek(findPositionForOffset(segment, currentOffset));
                    while (currentOffset < segmentEnd) {
                        int size = log.readInt();
                        // Always return at least one message, even if it is larger than maxBytes
                        if (!messages.isEmpty() && bytesRead + size > maxBytes) {
                            return messages;
                        }
                        byte[] message = new byte[size];
                        log.readFully(message);
                        messages.add(message);
                        bytesRead += size;
                        currentOffset++;
                    }
                }
                segmentIndex++;
            }
            return messages;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---------------------------------------------------------------- segments

    private void createNewSegment(long baseOffset) throws IOException {
        File logFile = new File(baseDir, segmentName(baseOffset) + ".log");
        File indexFile = new File(baseDir, segmentName(baseOffset) + ".index");
        SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
        segments.add(segment);
        openSegmentForAppend(segment);
        LOGGER.fine("Partition " + id + " created segment " + baseOffset);
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        closeActiveSegment();
        activeLogFile = new RandomAccessFile(segment.logFile, "rw");
        activeLogChannel = activeLogFile.getChannel();
        activeIndexFile = new RandomAccessFile(segment.indexFile, "rw");
        activeIndexChannel = activeIndexFile.getChannel();
    }

    /** Binary search for the segment that contains {@code offset}. */
    private int findSegmentForOffset(long offset) {
        int low = 0;
        int high = segments.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long start = segments.get(mid).baseOffset;
            long end = mid + 1 < segments.size() ? segments.get(mid + 1).baseOffset : Long.MAX_VALUE;
            if (offset < start) {
                high = mid - 1;
            } else if (offset >= end) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return 0;
    }

    /** Looks up a message's file position in the segment's index. */
    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {
        long relativeOffset = offset - segment.baseOffset;
        try (RandomAccessFile index = new RandomAccessFile(segment.indexFile, "r")) {
            long entryPosition = relativeOffset * INDEX_ENTRY_SIZE;
            if (entryPosition + INDEX_ENTRY_SIZE > index.length()) {
                return 0;
            }
            index.seek(entryPosition + 8);
            return index.readLong();
        }
    }

    private static String segmentName(long baseOffset) {
        return String.format("%020d", baseOffset);
    }

    private void closeActiveSegment() throws IOException {
        if (activeLogFile != null) {
            activeLogFile.close();
        }
        if (activeIndexFile != null) {
            activeIndexFile.close();
        }
    }

    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            closeActiveSegment();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---------------------------------------------------------------- accessors

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getFollowers() {
        return followers;
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    public int getSegmentCount() {
        return segments.size();
    }
}
