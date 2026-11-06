package com.tributary.client;

import com.tributary.common.Protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/** A lazily-opened, synchronous request/response connection to one broker. */
final class BrokerConnection implements Closeable {
    private final String host;
    private final int port;
    private final int timeoutMs;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    BrokerConnection(String host, int port, int timeoutMs) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
    }

    synchronized ByteBuffer send(ByteBuffer request) throws IOException {
        if (socket == null) {
            connect();
        }
        try {
            Protocol.writeFrame(out, request);
            return Protocol.readFrame(in);
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    private void connect() throws IOException {
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            s.setSoTimeout(timeoutMs);
        } catch (IOException e) {
            s.close();
            throw new IOException("Cannot connect to broker at " + host + ":" + port + ": " + e.getMessage(), e);
        }
        socket = s;
        in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 64 * 1024));
        out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 64 * 1024));
    }

    @Override
    public synchronized void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
            socket = null;
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
