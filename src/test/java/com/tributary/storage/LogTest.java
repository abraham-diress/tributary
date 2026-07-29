package com.tributary.storage;

import com.tributary.common.OffsetOutOfRangeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTest {
    @TempDir
    File dir;

    private static LogConfig smallSegments() {
        LogConfig c = new LogConfig();
        c.segmentBytes = 2048;
        c.indexIntervalBytes = 256;
        return c;
    }

    private static List<Record> batch(int from, int count) {
        List<Record> records = new ArrayList<>();
        for (int i = from; i < from + count; i++) {
            records.add(Record.of("key-" + i, "value-" + i));
        }
        return records;
    }

    private static List<Record> readAll(Log log, long from) throws IOException {
        List<Record> out = new ArrayList<>();
        long offset = from;
        while (offset < log.logEndOffset()) {
            List<Record> chunk = Records.decode(log.read(offset, 1000, Long.MAX_VALUE));
            out.addAll(chunk);
            offset = chunk.get(chunk.size() - 1).offset() + 1;
        }
        return out;
    }

    @Test
    void appendAssignsSequentialOffsetsAndReadsBack() throws IOException {
        try (Log log = new Log(dir, new LogConfig())) {
            assertEquals(0, log.append(batch(0, 10)));
            assertEquals(10, log.append(batch(10, 5)));
            assertEquals(15, log.logEndOffset());
            List<Record> all = readAll(log, 0);
            assertEquals(15, all.size());
            for (int i = 0; i < 15; i++) {
                assertEquals(i, all.get(i).offset());
                assertEquals("value-" + i, all.get(i).valueAsString());
            }
        }
    }

    @Test
    void nullKeysAndValuesRoundTrip() throws IOException {
        try (Log log = new Log(dir, new LogConfig())) {
            log.append(List.of(new Record(-1, 123L, null, null), new Record(-1, 456L, new byte[0], "x".getBytes())));
            List<Record> records = Records.decode(log.read(0, 4096, Long.MAX_VALUE));
            assertNull(records.get(0).key());
            assertNull(records.get(0).value());
            assertEquals(123L, records.get(0).timestamp());
            assertArrayEquals(new byte[0], records.get(1).key());
        }
    }

    @Test
    void rollsSegmentsAndReadsAcrossBoundaries() throws IOException {
        try (Log log = new Log(dir, smallSegments())) {
            for (int i = 0; i < 500; i++) {
                log.append(batch(i, 1));
            }
            assertTrue(log.numberOfSegments() > 5, "expected several segments, got " + log.numberOfSegments());
            List<Record> all = readAll(log, 0);
            assertEquals(500, all.size());
            for (int i = 0; i < 500; i++) {
                assertEquals(i, all.get(i).offset());
            }
        }
    }

    @Test
    void sparseIndexFindsArbitraryOffsets() throws IOException {
        try (Log log = new Log(dir, smallSegments())) {
            for (int i = 0; i < 1000; i += 10) {
                log.append(batch(i, 10));
            }
            for (long target : new long[]{0, 1, 9, 10, 257, 511, 998, 999}) {
                Record first = Records.decode(log.read(target, 100, Long.MAX_VALUE)).get(0);
                assertEquals(target, first.offset());
                assertEquals("value-" + target, first.valueAsString());
            }
        }
    }

    @Test
    void readRespectsMaxOffsetButAlwaysReturnsOneRecord() throws IOException {
        try (Log log = new Log(dir, new LogConfig())) {
            log.append(List.of(Record.of(null, new byte[5000])));
            log.append(batch(1, 10));
            assertEquals(1, Records.decode(log.read(0, 10, Long.MAX_VALUE)).size(), "oversized record still returned");
            List<Record> upToHw = Records.decode(log.read(1, 1 << 20, 5));
            assertEquals(4, upToHw.size());
            assertEquals(0, log.read(5, 1 << 20, 5).remaining());
        }
    }

    @Test
    void outOfRangeOffsetsAreRejected() throws IOException {
        try (Log log = new Log(dir, new LogConfig())) {
            log.append(batch(0, 3));
            assertEquals(0, log.read(3, 100, Long.MAX_VALUE).remaining(), "reading at the end is empty, not an error");
            OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () -> log.read(4, 100, Long.MAX_VALUE));
            assertEquals(0, e.logStartOffset());
            assertEquals(3, e.logEndOffset());
        }
    }

    @Test
    void recoversStateAfterReopen() throws IOException {
        try (Log log = new Log(dir, smallSegments())) {
            for (int i = 0; i < 300; i++) log.append(batch(i, 1));
        }
        try (Log log = new Log(dir, smallSegments())) {
            assertEquals(300, log.logEndOffset());
            assertEquals(300, readAll(log, 0).size());
            assertEquals(300, log.append(batch(300, 1)));
        }
    }

    @Test
    void truncatesTornWriteOnRecovery() throws IOException {
        long sizeBefore;
        try (Log log = new Log(dir, new LogConfig())) {
            log.append(batch(0, 20));
            sizeBefore = log.sizeInBytes();
        }
        // Simulate a crash mid-write: a partial record header + garbage at the end of the segment.
        File segment = new File(dir, String.format("%020d.log", 0));
        try (RandomAccessFile raf = new RandomAccessFile(segment, "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 1, 0, 42, 42, 42});
        }
        try (Log log = new Log(dir, new LogConfig())) {
            assertEquals(20, log.logEndOffset());
            assertEquals(sizeBefore, log.sizeInBytes());
            assertEquals(20, log.append(batch(20, 1)));
        }
    }

    @Test
    void detectsCorruptRecordViaCrc() throws IOException {
        try (Log log = new Log(dir, new LogConfig())) {
            log.append(batch(0, 10));
        }
        File segment = new File(dir, String.format("%020d.log", 0));
        try (RandomAccessFile raf = new RandomAccessFile(segment, "rw")) {
            long middle = raf.length() / 2;
            raf.seek(middle);
            int b = raf.read();
            raf.seek(middle);
            raf.write(b ^ 0xFF); // flip bits inside some record
        }
        try (Log log = new Log(dir, new LogConfig())) {
            assertTrue(log.logEndOffset() < 10, "corrupt record and everything after it should be dropped");
            for (Record r : readAll(log, 0)) {
                assertEquals("value-" + r.offset(), r.valueAsString());
            }
        }
    }

    @Test
    void truncateToRemovesSuffixAndContinuesOffsets() throws IOException {
        try (Log log = new Log(dir, smallSegments())) {
            for (int i = 0; i < 200; i++) log.append(batch(i, 1));
            int segmentsBefore = log.numberOfSegments();
            log.truncateTo(57);
            assertEquals(57, log.logEndOffset());
            assertTrue(log.numberOfSegments() < segmentsBefore);
            assertEquals(57, readAll(log, 0).size());
            assertEquals(57, log.append(batch(1000, 1)));
            assertEquals("value-1000", Records.decode(log.read(57, 100, Long.MAX_VALUE)).get(0).valueAsString());
        }
        try (Log log = new Log(dir, smallSegments())) {
            assertEquals(58, log.logEndOffset());
        }
    }

    @Test
    void followerAppendRequiresContiguousOffsets() throws IOException {
        try (Log leader = new Log(new File(dir, "leader"), new LogConfig());
             Log follower = new Log(new File(dir, "follower"), new LogConfig())) {
            leader.append(batch(0, 10));
            ByteBuffer raw = leader.read(0, 1 << 20, Long.MAX_VALUE);
            follower.appendAsFollower(raw);
            assertEquals(10, follower.logEndOffset());
            assertThrows(IllegalStateException.class, () -> follower.appendAsFollower(leader.read(5, 1 << 20, Long.MAX_VALUE)));
        }
    }

    @Test
    void sizeBasedRetentionDeletesOldestSegments() throws IOException {
        LogConfig config = smallSegments();
        config.retentionBytes = 4096;
        try (Log log = new Log(dir, config)) {
            for (int i = 0; i < 500; i++) log.append(batch(i, 1));
            int deleted = log.deleteOldSegments();
            assertTrue(deleted > 0);
            assertTrue(log.logStartOffset() > 0);
            assertTrue(log.sizeInBytes() < 4096 + config.segmentBytes);
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(0, 100, Long.MAX_VALUE));
            assertEquals(log.logStartOffset(), Records.decode(log.read(log.logStartOffset(), 100, Long.MAX_VALUE)).get(0).offset());
        }
    }
}
