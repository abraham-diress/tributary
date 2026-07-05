package com.tributary.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Sparse offset index for one segment. Each 8-byte entry maps a segment-relative offset (int32) to
 * the byte position (int32) of that record in the .log file. Only about one record per
 * {@code indexIntervalBytes} is indexed; a lookup binary-searches for the closest entry at or
 * before the target and the segment scans forward from there.
 *
 * <p>Entries are mirrored in memory for lookups and appended to disk so restarts don't need to
 * rebuild the index for closed segments.
 */
final class OffsetIndex implements Closeable {
    private static final int ENTRY_SIZE = 8;

    private final File file;
    private final long baseOffset;
    private final FileChannel channel;
    private int[] relativeOffsets = new int[64];
    private int[] positions = new int[64];
    private int entries;

    OffsetIndex(File file, long baseOffset) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.channel = FileChannel.open(file.toPath(), CREATE, READ, WRITE);
        load();
    }

    private void load() throws IOException {
        long length = channel.size();
        int n = (int) (length / ENTRY_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(n * ENTRY_SIZE);
        IoUtil.readFully(channel, buf, 0);
        buf.flip();
        int prevRel = -1;
        int prevPos = -1;
        for (int i = 0; i < n; i++) {
            int rel = buf.getInt();
            int pos = buf.getInt();
            if (rel <= prevRel || pos <= prevPos) {
                break; // torn or corrupt tail: keep the valid prefix
            }
            addInMemory(rel, pos);
            prevRel = rel;
            prevPos = pos;
        }
        if ((long) entries * ENTRY_SIZE != length) {
            channel.truncate((long) entries * ENTRY_SIZE);
        }
    }

    void append(long offset, int position) throws IOException {
        int rel = (int) (offset - baseOffset);
        if (entries > 0 && (rel <= relativeOffsets[entries - 1] || position <= positions[entries - 1])) {
            return;
        }
        ByteBuffer entry = ByteBuffer.allocate(ENTRY_SIZE).putInt(rel).putInt(position).flip();
        IoUtil.writeFully(channel, entry, (long) entries * ENTRY_SIZE);
        addInMemory(rel, position);
    }

    private void addInMemory(int rel, int pos) {
        if (entries == relativeOffsets.length) {
            relativeOffsets = Arrays.copyOf(relativeOffsets, entries * 2);
            positions = Arrays.copyOf(positions, entries * 2);
        }
        relativeOffsets[entries] = rel;
        positions[entries] = pos;
        entries++;
    }

    /** Byte position of the largest indexed offset that is <= {@code target}, or 0 if none. */
    int lookup(long target) {
        long rel = target - baseOffset;
        int lo = 0;
        int hi = entries - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (relativeOffsets[mid] <= rel) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found < 0 ? 0 : positions[found];
    }

    /** Drops entries that point at or past {@code logSize} after the log was truncated. */
    void truncateToPosition(int logSize) throws IOException {
        while (entries > 0 && positions[entries - 1] >= logSize) {
            entries--;
        }
        channel.truncate((long) entries * ENTRY_SIZE);
    }

    void reset() throws IOException {
        entries = 0;
        channel.truncate(0);
    }

    int entries() {
        return entries;
    }

    void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    void delete() throws IOException {
        close();
        Files.deleteIfExists(file.toPath());
    }
}
