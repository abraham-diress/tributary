package com.tributary.client;

import java.nio.charset.StandardCharsets;

public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value) {
    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
