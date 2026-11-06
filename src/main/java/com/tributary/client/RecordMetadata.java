package com.tributary.client;

/** Where a produced record was written. */
public record RecordMetadata(String topic, int partition, long offset) {
}
