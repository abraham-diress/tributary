package com.tributary.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

final class IoUtil {
    private IoUtil() {
    }

    static void readFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new EOFException("Unexpected end of file at position " + pos);
            }
            pos += n;
        }
    }

    static void writeFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            pos += channel.write(buf, pos);
        }
    }
}
