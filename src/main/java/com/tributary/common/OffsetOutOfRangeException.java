package com.tributary.common;

/** Thrown when a fetch asks for an offset outside [logStartOffset, logEndOffset]. */
public class OffsetOutOfRangeException extends KafkaException {
    private final long logStartOffset;
    private final long logEndOffset;

    public OffsetOutOfRangeException(long requested, long logStartOffset, long logEndOffset) {
        this("Offset " + requested + " is outside the log range [" + logStartOffset + ", " + logEndOffset + "]",
                logStartOffset, logEndOffset);
    }

    public OffsetOutOfRangeException(String detail, long logStartOffset, long logEndOffset) {
        super(Errors.OFFSET_OUT_OF_RANGE, detail);
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long logEndOffset() {
        return logEndOffset;
    }
}
