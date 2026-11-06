package com.tributary.client;

@FunctionalInterface
public interface MessageHandler {
    void handle(ConsumerRecord record) throws Exception;
}
