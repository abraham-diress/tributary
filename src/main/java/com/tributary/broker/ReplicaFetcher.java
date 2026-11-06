package com.tributary.broker;

import com.tributary.common.BrokerInfo;
import com.tributary.common.KafkaException;
import com.tributary.common.OffsetOutOfRangeException;
import com.tributary.common.Protocol;
import com.tributary.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;

/**
 * Pull-based replication for one follower partition: repeatedly long-polls the leader with a FETCH
 * from this replica's log end offset and appends whatever comes back. Pulling (rather than the
 * leader pushing) lets a follower that was offline catch up at its own pace and keeps the leader
 * stateless about in-flight sends.
 *
 * <p>The thread is never interrupted on shutdown: interrupting a thread in the middle of
 * {@code FileChannel} I/O closes the channel. Instead the socket is closed, which unblocks reads.
 */
final class ReplicaFetcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcher.class);

    private final Partition partition;
    private final TopicPartition tp;
    private final int localBrokerId;
    private final int leaderId;
    private final int leaderEpoch;
    private final IntFunction<BrokerInfo> brokerLookup;
    private final BrokerConfig config;
    private final Thread thread;
    private volatile boolean running = true;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    ReplicaFetcher(Partition partition, int leaderId, int leaderEpoch, IntFunction<BrokerInfo> brokerLookup, BrokerConfig config) {
        this.partition = partition;
        this.tp = partition.topicPartition();
        this.localBrokerId = config.brokerId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.brokerLookup = brokerLookup;
        this.config = config;
        this.thread = new Thread(this, "replica-fetcher-" + tp + "-from-" + leaderId);
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    void shutdown() {
        running = false;
        closeSocket();
        try {
            thread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    int leaderId() {
        return leaderId;
    }

    int leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public void run() {
        LOG.debug("{}: started fetching from leader {}", tp, leaderId);
        while (running) {
            long offset = partition.log().logEndOffset();
            try {
                ensureConnected();
                ByteBuffer request = Protocol.encodeFetchRequest(tp.topic(), tp.partition(), offset,
                        config.replicaFetchMaxBytes, config.replicaFetchWaitMs, localBrokerId, leaderEpoch);
                Protocol.writeFrame(out, request);
                Protocol.FetchResult result = Protocol.decodeFetchResponse(Protocol.readFrame(in));
                if (!running) {
                    break;
                }
                if (result.rawRecords().hasRemaining()) {
                    partition.appendAsFollower(result.rawRecords());
                }
                partition.updateFollowerHighWatermark(result.highWatermark());
            } catch (OffsetOutOfRangeException e) {
                handleOutOfRange(offset, e);
            } catch (KafkaException e) {
                // e.g. the leader hasn't processed the new partition state yet; ZooKeeper events
                // will restart this fetcher if leadership actually moved.
                LOG.debug("{}: fetch from leader {} failed: {}", tp, leaderId, e.getMessage());
                backoff(200);
            } catch (IOException e) {
                if (running) {
                    LOG.debug("{}: connection to leader {} failed: {}", tp, leaderId, e.getMessage());
                }
                closeSocket();
                backoff(500);
            } catch (RuntimeException e) {
                LOG.error("{}: unexpected error replicating from leader {}", tp, leaderId, e);
                backoff(1000);
            }
        }
        closeSocket();
    }

    private void handleOutOfRange(long offset, OffsetOutOfRangeException e) {
        try {
            if (offset > e.logEndOffset()) {
                LOG.warn("{}: local log ({}) is ahead of leader ({}), truncating", tp, offset, e.logEndOffset());
                partition.truncateTo(e.logEndOffset());
            } else if (offset < e.logStartOffset()) {
                LOG.warn("{}: leader's log starts at {} (retention), resetting local log", tp, e.logStartOffset());
                partition.truncateFullyAndStartAt(e.logStartOffset());
            }
        } catch (IOException ex) {
            LOG.error("{}: failed to truncate log", tp, ex);
            backoff(1000);
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null) {
            return;
        }
        BrokerInfo leader = brokerLookup.apply(leaderId);
        if (leader == null) {
            throw new IOException("Leader " + leaderId + " is not registered");
        }
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.connect(new InetSocketAddress(leader.getHost(), leader.getPort()), 5000);
        s.setSoTimeout(config.replicaFetchWaitMs + 30_000);
        socket = s;
        in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 64 * 1024));
        out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 64 * 1024));
    }

    private synchronized void closeSocket() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
            socket = null;
        }
    }

    private void backoff(long ms) {
        long deadline = System.currentTimeMillis() + ms;
        while (running && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(Math.min(50, ms));
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
