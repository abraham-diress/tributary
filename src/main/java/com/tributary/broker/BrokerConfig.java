package com.tributary.broker;

import com.tributary.storage.LogConfig;

/**
 * Broker settings. The required ones come from the command line; tuning knobs can be overridden
 * with system properties, e.g. {@code -Dtributary.min.insync.replicas=2}.
 */
public final class BrokerConfig {
    public final int brokerId;
    public final String host;
    public final int port;
    public final String zkConnect;
    public final String dataDir;

    /** Followers that haven't caught up to the leader within this window are removed from the ISR. */
    public long replicaLagMaxMs = Long.getLong("tributary.replica.lag.max.ms", 10_000);
    /** Minimum ISR size for an acks=all write to be accepted. */
    public int minInsyncReplicas = Integer.getInteger("tributary.min.insync.replicas", 1);
    public int replicaFetchMaxBytes = Integer.getInteger("tributary.replica.fetch.max.bytes", 1024 * 1024);
    public int replicaFetchWaitMs = Integer.getInteger("tributary.replica.fetch.wait.ms", 500);
    public long flushIntervalMs = Long.getLong("tributary.log.flush.interval.ms", 1000);
    public long retentionCheckIntervalMs = Long.getLong("tributary.log.retention.check.interval.ms", 30_000);
    public int zkSessionTimeoutMs = Integer.getInteger("tributary.zk.session.timeout.ms", 6000);
    public final LogConfig logConfig = new LogConfig();

    public BrokerConfig(int brokerId, String host, int port, String zkConnect, String dataDir) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.zkConnect = zkConnect;
        this.dataDir = dataDir;
        logConfig.segmentBytes = Integer.getInteger("tributary.log.segment.bytes", logConfig.segmentBytes);
        logConfig.retentionMs = Long.getLong("tributary.log.retention.ms", logConfig.retentionMs);
        logConfig.retentionBytes = Long.getLong("tributary.log.retention.bytes", logConfig.retentionBytes);
        logConfig.flushOnEveryAppend = Boolean.getBoolean("tributary.log.flush.every.append");
    }
}
