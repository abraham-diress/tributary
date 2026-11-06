package com.tributary.common;

import java.io.IOException;

/** An error reported by a broker, identified by one of the codes in {@link Errors}. */
public class KafkaException extends IOException {
    private final short code;
    private final String detail;

    public KafkaException(short code, String detail) {
        super(detail == null ? Errors.name(code) : Errors.name(code) + ": " + detail);
        this.code = code;
        this.detail = detail;
    }

    public short code() {
        return code;
    }

    /** The message without the error-name prefix, as sent over the wire. */
    public String detail() {
        return detail;
    }

    public boolean isRetriable() {
        return Errors.isRetriable(code);
    }
}
