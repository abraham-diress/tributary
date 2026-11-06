package com.tributary.broker;

import com.tributary.common.Errors;
import com.tributary.common.KafkaException;
import com.tributary.common.OffsetOutOfRangeException;
import com.tributary.common.TopicPartition;
import com.tributary.storage.Log;
import com.tributary.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A broker's local replica of one partition: its {@link Log} plus replication state.
 *
 * <p><b>As leader</b> it tracks each follower's log end offset (learned from the offset the follower
 * fetches at) and the in-sync replica set (ISR). The <em>high watermark</em> (HW) is the smallest log
 * end offset across the ISR: every record below it is on all in-sync replicas and is therefore
 * committed. Consumers only ever see committed records, and acks=all producers wait for the HW to
 * pass their write.
 *
 * <p><b>As follower</b> a {@link ReplicaFetcher} pulls from the leader. On becoming a follower the
 * log is truncated to the local HW so any uncommitted suffix that the new leader may not have is
 * discarded and re-fetched.
 *
 * <p>All replication state is guarded by this object's monitor, which also serves as the
 * condition variable for long-polling fetches and acks=all producers.
 */
public class Partition {
    private static final Logger LOG = LoggerFactory.getLogger(Partition.class);
    private static final String HW_CHECKPOINT = "high-watermark.checkpoint";

    /** Persists ISR changes to ZooKeeper; returns false if the change was rejected. */
    public interface IsrUpdater {
        boolean updateIsr(Partition partition, int leaderEpoch, Set<Integer> newIsr);
    }

    public record FetchData(long highWatermark, long logStartOffset, long logEndOffset, ByteBuffer records) {
    }

    private static final class FollowerState {
        long logEndOffset = -1;
        long lastFetchLeaderLogEndOffset;
        long lastFetchTimeMs;
        long lastCaughtUpTimeMs;
    }

    private final TopicPartition tp;
    private final int localBrokerId;
    private final Log log;
    private final IsrUpdater isrUpdater;
    private final File hwCheckpointFile;

    private volatile boolean isLeader;
    private volatile int leaderId = -1;
    private volatile int leaderEpoch = -1;
    private volatile List<Integer> replicas = List.of();
    private volatile Set<Integer> isr = Set.of();
    private volatile long highWatermark;
    private final Map<Integer, FollowerState> followers = new HashMap<>();

    public Partition(TopicPartition tp, int localBrokerId, Log log, IsrUpdater isrUpdater) {
        this.tp = tp;
        this.localBrokerId = localBrokerId;
        this.log = log;
        this.isrUpdater = isrUpdater;
        this.hwCheckpointFile = new File(log.dir(), HW_CHECKPOINT);
        this.highWatermark = loadHighWatermark();
    }

    // ------------------------------------------------------------------ leadership transitions

    public synchronized void makeLeader(PartitionState state) {
        isLeader = true;
        leaderId = localBrokerId;
        leaderEpoch = state.leaderEpoch();
        replicas = state.replicas();
        isr = new HashSet<>(state.isr());
        followers.clear();
        long now = System.currentTimeMillis();
        for (int r : replicas) {
            if (r != localBrokerId) {
                FollowerState f = new FollowerState();
                f.lastCaughtUpTimeMs = now; // grace period before it can be dropped from the ISR
                followers.put(r, f);
            }
        }
        LOG.info("{}: became leader at epoch {} with ISR {} (LEO={}, HW={})",
                tp, leaderEpoch, isr, log.logEndOffset(), highWatermark);
        maybeAdvanceHighWatermark();
        notifyAll();
    }

    public synchronized void makeFollower(PartitionState state) throws IOException {
        isLeader = false;
        leaderId = state.leader();
        leaderEpoch = state.leaderEpoch();
        replicas = state.replicas();
        isr = new HashSet<>(state.isr());
        followers.clear();
        if (log.logEndOffset() > highWatermark) {
            log.truncateTo(highWatermark);
        }
        LOG.info("{}: became follower of broker {} at epoch {} (LEO={})", tp, leaderId, leaderEpoch, log.logEndOffset());
        notifyAll();
    }

    /** Applies an ISR change that didn't change leadership (same epoch). */
    public synchronized void updateState(PartitionState state) {
        if (state.leaderEpoch() != leaderEpoch) {
            return;
        }
        replicas = state.replicas();
        isr = new HashSet<>(state.isr());
        if (isLeader) {
            maybeAdvanceHighWatermark();
        }
    }

    synchronized void applyIsr(int epoch, Set<Integer> newIsr) {
        if (epoch == leaderEpoch) {
            isr = new HashSet<>(newIsr);
            if (isLeader) {
                maybeAdvanceHighWatermark();
            }
        }
    }

    // ------------------------------------------------------------------ leader path

    public long appendAsLeader(List<Record> records) throws IOException {
        if (!isLeader) {
            throw new KafkaException(Errors.NOT_LEADER, "Broker " + localBrokerId + " is not the leader for " + tp);
        }
        long baseOffset = log.append(records);
        synchronized (this) {
            maybeAdvanceHighWatermark();
            notifyAll();
        }
        return baseOffset;
    }

    /** Waits until the HW reaches {@code offset}. Returns false on timeout or loss of leadership. */
    public synchronized boolean awaitHighWatermark(long offset, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (highWatermark < offset) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0 || !isLeader) {
                return false;
            }
            wait(remaining);
        }
        return true;
    }

    /** Serves a consumer fetch. Only committed records (below the HW) are visible. */
    public FetchData readForConsumer(long offset, int maxBytes, int maxWaitMs) throws IOException, InterruptedException {
        checkLeader();
        checkOffsetRange(offset);
        if (maxWaitMs > 0) {
            synchronized (this) {
                long deadline = System.currentTimeMillis() + maxWaitMs;
                long remaining;
                while (highWatermark <= offset && isLeader && (remaining = deadline - System.currentTimeMillis()) > 0) {
                    wait(remaining);
                }
            }
        }
        long hw = highWatermark;
        ByteBuffer records = log.read(offset, maxBytes, hw);
        return new FetchData(hw, log.logStartOffset(), log.logEndOffset(), records);
    }

    /**
     * Serves a follower's replication fetch. The fetch offset tells us the follower's log end
     * offset, which may advance the HW or qualify the follower to rejoin the ISR.
     */
    public FetchData readForFollower(int replicaId, long offset, int maxBytes, int maxWaitMs)
            throws IOException, InterruptedException {
        checkLeader();
        long leaderLogEnd = checkOffsetRange(offset);
        Set<Integer> expandedIsr = null;
        int epoch;
        synchronized (this) {
            FollowerState f = followers.get(replicaId);
            if (f == null) {
                throw new KafkaException(Errors.INVALID_REQUEST, "Broker " + replicaId + " is not a replica of " + tp);
            }
            long now = System.currentTimeMillis();
            if (offset >= leaderLogEnd) {
                f.lastCaughtUpTimeMs = now;
            } else if (offset >= f.lastFetchLeaderLogEndOffset) {
                // Caught up to where the leader was at the previous fetch: in sync as of that time.
                f.lastCaughtUpTimeMs = Math.max(f.lastCaughtUpTimeMs, f.lastFetchTimeMs);
            }
            f.lastFetchLeaderLogEndOffset = leaderLogEnd;
            f.lastFetchTimeMs = now;
            f.logEndOffset = offset;
            if (!isr.contains(replicaId) && offset >= highWatermark) {
                expandedIsr = new HashSet<>(isr);
                expandedIsr.add(replicaId);
            }
            epoch = leaderEpoch;
            maybeAdvanceHighWatermark();
        }
        if (expandedIsr != null && isrUpdater.updateIsr(this, epoch, expandedIsr)) {
            LOG.info("{}: expanded ISR to {} (broker {} caught up)", tp, expandedIsr, replicaId);
        }
        if (maxWaitMs > 0) {
            synchronized (this) {
                long deadline = System.currentTimeMillis() + maxWaitMs;
                long remaining;
                while (log.logEndOffset() <= offset && isLeader && (remaining = deadline - System.currentTimeMillis()) > 0) {
                    wait(remaining);
                }
            }
        }
        ByteBuffer records = log.read(offset, maxBytes, Long.MAX_VALUE);
        return new FetchData(highWatermark, log.logStartOffset(), log.logEndOffset(), records);
    }

    /** In-sync followers that haven't caught up within {@code maxLagMs}. */
    public synchronized Set<Integer> laggingReplicas(long nowMs, long maxLagMs) {
        Set<Integer> lagging = new HashSet<>();
        if (!isLeader) {
            return lagging;
        }
        for (int r : isr) {
            if (r == localBrokerId) continue;
            FollowerState f = followers.get(r);
            if (f == null || nowMs - f.lastCaughtUpTimeMs > maxLagMs) {
                lagging.add(r);
            }
        }
        return lagging;
    }

    /** HW = min log end offset over the ISR. It never moves backwards. Caller holds the monitor. */
    private void maybeAdvanceHighWatermark() {
        long min = log.logEndOffset();
        for (int r : isr) {
            if (r == localBrokerId) continue;
            FollowerState f = followers.get(r);
            if (f == null || f.logEndOffset < 0) {
                return; // an in-sync follower hasn't reported its position yet
            }
            min = Math.min(min, f.logEndOffset);
        }
        if (min > highWatermark) {
            highWatermark = min;
            notifyAll();
        }
    }

    // ------------------------------------------------------------------ follower path

    public void appendAsFollower(ByteBuffer records) throws IOException {
        log.appendAsFollower(records);
        synchronized (this) {
            notifyAll();
        }
    }

    public synchronized void updateFollowerHighWatermark(long leaderHighWatermark) {
        long hw = Math.min(leaderHighWatermark, log.logEndOffset());
        if (hw > highWatermark) {
            highWatermark = hw;
            notifyAll();
        }
    }

    public synchronized void truncateTo(long offset) throws IOException {
        log.truncateTo(offset);
        highWatermark = Math.min(highWatermark, log.logEndOffset());
    }

    public synchronized void truncateFullyAndStartAt(long offset) throws IOException {
        log.truncateFullyAndStartAt(offset);
        highWatermark = offset;
    }

    // ------------------------------------------------------------------ helpers

    private void checkLeader() throws KafkaException {
        if (!isLeader) {
            throw new KafkaException(Errors.NOT_LEADER, "Broker " + localBrokerId + " is not the leader for " + tp);
        }
    }

    private long checkOffsetRange(long offset) throws OffsetOutOfRangeException {
        long start = log.logStartOffset();
        long end = log.logEndOffset();
        if (offset < start || offset > end) {
            throw new OffsetOutOfRangeException(offset, start, end);
        }
        return end;
    }

    public synchronized void wakeWaiters() {
        notifyAll();
    }

    /** Persists the HW so a restarted follower knows how much of its log is safe to keep. */
    public void checkpointHighWatermark() {
        try {
            File tmp = new File(log.dir(), HW_CHECKPOINT + ".tmp");
            Files.writeString(tmp.toPath(), Long.toString(highWatermark));
            Files.move(tmp.toPath(), hwCheckpointFile.toPath(), StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.warn("{}: failed to checkpoint high watermark", tp, e);
        }
    }

    private long loadHighWatermark() {
        try {
            if (hwCheckpointFile.exists()) {
                long hw = Long.parseLong(Files.readString(hwCheckpointFile.toPath(), StandardCharsets.UTF_8).trim());
                return Math.max(log.logStartOffset(), Math.min(hw, log.logEndOffset()));
            }
        } catch (IOException | NumberFormatException e) {
            LOG.warn("{}: ignoring unreadable high watermark checkpoint", tp, e);
        }
        return log.logStartOffset();
    }

    public TopicPartition topicPartition() {
        return tp;
    }

    public Log log() {
        return log;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public int leaderId() {
        return leaderId;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public Set<Integer> inSyncReplicas() {
        return Set.copyOf(isr);
    }

    public List<Integer> replicas() {
        return replicas;
    }
}
