package com.tributary.storage;

import java.nio.charset.StandardCharsets;

/**
 * A single message. {@code offset} is -1 until the partition leader assigns one on append.
 * Key and value may each be null.
 */
public record Record(long offset, long timestamp, byte[] key, byte[] value) {

    public static Record of(byte[] key, byte[] value) {
        return new Record(-1, System.currentTimeMillis(), key, value);
    }

    public static Record of(String key, String value) {
        return of(key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
