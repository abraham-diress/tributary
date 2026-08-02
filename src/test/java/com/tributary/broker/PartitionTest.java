package com.tributary.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionTest {
    @TempDir
    File dir;

    private Partition open() throws Exception {
        return new Partition(0, 1, List.of(2, 3), dir.getPath());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void appendsAndReadsMessagesInOrder() throws Exception {
        Partition partition = open();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, partition.append(bytes("message-" + i)));
        }
        List<byte[]> messages = partition.readMessages(0, 1024 * 1024);
        assertEquals(100, messages.size());
        assertEquals("message-0", str(messages.get(0)));
        assertEquals("message-99", str(messages.get(99)));
        assertEquals("message-42", str(partition.readMessages(42, 1024 * 1024).get(0)));
        partition.close();
    }

    @Test
    void readingPastTheEndReturnsNothing() throws Exception {
        Partition partition = open();
        partition.append(bytes("only"));
        assertTrue(partition.readMessages(1, 1024).isEmpty());
        assertTrue(partition.readMessages(100, 1024).isEmpty());
        partition.close();
    }

    @Test
    void respectsMaxBytesButReturnsAtLeastOneMessage() throws Exception {
        Partition partition = open();
        for (int i = 0; i < 10; i++) {
            partition.append(new byte[100]);
        }
        assertEquals(3, partition.readMessages(0, 300).size());
        assertEquals(1, partition.readMessages(0, 10).size());
        partition.close();
    }

    @Test
    void rollsToNewSegmentsAndReadsAcrossThem() throws Exception {
        Partition partition = open();
        byte[] big = new byte[100 * 1024];
        for (int i = 0; i < 25; i++) {
            big[0] = (byte) i;
            partition.append(big);
        }
        assertTrue(partition.getSegmentCount() > 1);
        List<byte[]> messages = partition.readMessages(5, 10 * 1024 * 1024);
        assertEquals(20, messages.size());
        for (int i = 0; i < 20; i++) {
            assertEquals(i + 5, messages.get(i)[0]);
        }
        partition.close();
    }

    @Test
    void recoversAfterRestart() throws Exception {
        Partition partition = open();
        byte[] big = new byte[100 * 1024];
        for (int i = 0; i < 25; i++) {
            partition.append(big);
        }
        partition.close();

        Partition reopened = open();
        assertEquals(25, reopened.getNextOffset());
        assertEquals(25, reopened.append(bytes("after restart")));
        assertEquals("after restart", str(reopened.readMessages(25, 1024).get(0)));
        reopened.close();
    }

    @Test
    void cutsOffAPartlyWrittenMessageAfterACrash() throws Exception {
        Partition partition = open();
        partition.append(bytes("first"));
        partition.append(bytes("second"));
        partition.close();

        // Simulate a crash in the middle of a write: a length prefix with only some of the bytes
        try (RandomAccessFile log = new RandomAccessFile(new File(dir, String.format("%020d.log", 0)), "rw")) {
            log.seek(log.length());
            log.writeInt(50);
            log.write(new byte[10]);
        }

        Partition reopened = open();
        assertEquals(2, reopened.getNextOffset());
        assertEquals(2, reopened.append(bytes("third")));
        List<byte[]> messages = reopened.readMessages(0, 1024);
        assertEquals("third", str(messages.get(2)));
        reopened.close();
    }
}

