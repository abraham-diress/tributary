package com.tributary.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sends string messages to a topic, to a chosen or random partition.
 */
public class TributaryProducer {
    private final TributaryClient client;
    private final String topic;

    public TributaryProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new TributaryClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    /** Sends the message to a random partition and returns its offset. */
    public long send(String message) throws IOException {
        var metadata = client.getTopicMetadata(topic);
        if (metadata == null) {
            client.refreshMetadata();
            metadata = client.getTopicMetadata(topic);
        }
        if (metadata == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
        int numPartitions = metadata.getPartitions().size();
        return send(message, ThreadLocalRandom.current().nextInt(numPartitions));
    }

    /** Sends the message to the given partition and returns its offset. */
    public long send(String message, int partition) throws IOException {
        return client.send(topic, partition, message.getBytes(StandardCharsets.UTF_8));
    }

    public TributaryClient getClient() {
        return client;
    }

    public void close() {
        // Connections are opened per request, so there is nothing to release
    }

    /** Creates the topic if needed, sends 10 test messages, and prints where each one was written. */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TributaryProducer <host> <port> <topic> [count]");
            System.exit(1);
        }
        String topic = args[2];
        int count = args.length > 3 ? Integer.parseInt(args[3]) : 10;

        TributaryProducer producer = new TributaryProducer(args[0], Integer.parseInt(args[1]), topic);
        producer.initialize();
        TributaryClient client = producer.getClient();
        if (client.getTopicMetadata(topic) == null) {
            int replicationFactor = Math.min(3, client.getBrokerCount());
            System.out.println("Creating topic " + topic + " with 3 partitions, replication factor " + replicationFactor);
            client.createTopic(topic, 3, replicationFactor);
        }

        for (int i = 0; i < count; i++) {
            int partition = i % 3;
            String message = "Message " + i;
            long offset = producer.send(message, partition);
            System.out.println("Sent '" + message + "' to partition " + partition + " at offset " + offset);
        }
        producer.close();
    }
}
