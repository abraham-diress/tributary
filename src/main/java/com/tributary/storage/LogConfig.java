package com.tributary.storage;

/** Tunables for a partition's log. */
public final class LogConfig {
    /** Roll to a new segment once the active one would exceed this many bytes. */
    public int segmentBytes = 1024 * 1024;
    /** Add a sparse index entry roughly every this many bytes of log data. */
    public int indexIntervalBytes = 4096;
    /** fsync after every append (slow, maximally durable). Otherwise the broker flushes periodically. */
    public boolean flushOnEveryAppend = false;
    /** Delete closed segments older than this. -1 disables time-based retention. */
    public long retentionMs = 7L * 24 * 60 * 60 * 1000;
    /** Delete the oldest closed segments while the log is larger than this. -1 disables size-based retention. */
    public long retentionBytes = -1;
}
