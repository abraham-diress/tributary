package com.tributary.client;

@FunctionalInterface
public interface MessageHandler {
    void handle(byte[] message, long offset);
}

