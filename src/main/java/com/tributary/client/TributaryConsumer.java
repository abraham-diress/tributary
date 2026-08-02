package com.tributary.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads messages from one partition of a topic, keeping track of the next offset to read.
 */
public class TributaryConsumer {
    private static final Logger LOGGER = Logger.getLogger(TributaryConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024;
    private static final long POLL_INTERVAL_MS = 500;

    private final TributaryClient client;
    private final String topic;
    private final int partition;
    private volatile long currentOffset;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public TributaryConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this.client = new TributaryClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    /** Fetches the next batch of messages and moves the offset past them. */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);
        currentOffset += messages.size();
        return messages;
    }

    /** Polls in a background thread and passes each message to the handler. */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        long firstOffset = currentOffset;
                        List<byte[]> messages = poll();
                        for (int i = 0; i < messages.size(); i++) {
                            handler.handle(messages.get(i), firstOffset + i);
                        }
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        if (running.get()) {
                            LOGGER.log(Level.WARNING, "Error while consuming, retrying", e);
                        }
                    }
                }
            });
            consumerThread.setDaemon(true);
            consumerThread.start();
            LOGGER.info("Started consuming from topic " + topic + ", partition " + partition);
        }
    }

    public void stopConsuming() {
        if (running.compareAndSet(true, false) && consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void seek(long offset) {
        this.currentOffset = offset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    /** Prints every message in the partition, then keeps waiting for new ones. */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TributaryConsumer <host> <port> <topic> <partition>");
            System.exit(1);
        }
        TributaryConsumer consumer = new TributaryConsumer(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]));
        consumer.initialize();
        consumer.startConsuming((message, offset) ->
                System.out.println("Offset " + offset + ": " + new String(message, StandardCharsets.UTF_8)));
        Thread.currentThread().join();
    }
}

