package com.tributary.storage;

public class CorruptRecordException extends RuntimeException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
