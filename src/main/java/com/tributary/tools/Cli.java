package com.tributary.tools;

import com.tributary.client.ConsumerRecord;
import com.tributary.client.TributaryClient;
import com.tributary.client.TributaryConsumer;
import com.tributary.client.TributaryProducer;
import com.tributary.common.BrokerInfo;
import com.tributary.common.Protocol;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command-line tool.
 *
 * <pre>
 *   create-topic --topic T [--partitions 3] [--replication-factor 3]
 *   describe     [--topic T]
 *   produce      --topic T [--acks all|1|0] [--key-separator :]     (one message per stdin line)
 *   consume      --topic T [--group G] [--partition P] [--from-latest] [--max-messages N] [--idle-timeout-ms MS]
 * </pre>
 * Every command accepts {@code --bootstrap host:port[,host:port]} (default localhost:9091).
 */
public final class Cli {
    private Cli() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        Map<String, String> opts = parse(args);
        String bootstrap = opts.getOrDefault("bootstrap", "localhost:9091");
        switch (args[0]) {
            case "create-topic" -> createTopic(bootstrap, opts);
            case "describe" -> describe(bootstrap, opts);
            case "produce" -> produce(bootstrap, opts);
            case "consume" -> consume(bootstrap, opts);
            default -> usage();
        }
    }

    private static void createTopic(String bootstrap, Map<String, String> opts) throws Exception {
        try (TributaryClient client = new TributaryClient(bootstrap)) {
            client.initialize();
            String topic = require(opts, "topic");
            int partitions = Integer.parseInt(opts.getOrDefault("partitions", "3"));
            int rf = Integer.parseInt(opts.getOrDefault("replication-factor", "3"));
            client.createTopic(topic, partitions, rf);
            System.out.printf("Created topic '%s' (%d partitions, replication factor %d)%n", topic, partitions, rf);
        }
    }

    private static void describe(String bootstrap, Map<String, String> opts) throws Exception {
        try (TributaryClient client = new TributaryClient(bootstrap)) {
            client.initialize();
            Protocol.MetadataResult md = client.metadata();
            System.out.println("Brokers:");
            for (BrokerInfo b : md.brokers()) {
                System.out.printf("  %d  %s:%d%s%n", b.getId(), b.getHost(), b.getPort(),
                        b.getId() == md.controllerId() ? "  (controller)" : "");
            }
            for (Protocol.TopicMetadata t : md.topics()) {
                if (opts.containsKey("topic") && !t.name().equals(opts.get("topic"))) continue;
                System.out.printf("Topic: %s%n", t.name());
                for (Protocol.PartitionMetadata p : t.partitions()) {
                    System.out.printf("  partition %-3d leader %-3s epoch %-3d replicas %-12s isr %s%n", p.id(),
                            p.leader() < 0 ? "none" : String.valueOf(p.leader()), p.leaderEpoch(), p.replicas(), p.isr());
                }
            }
        }
    }

    private static void produce(String bootstrap, Map<String, String> opts) throws Exception {
        String topic = require(opts, "topic");
        TributaryProducer.Config config = new TributaryProducer.Config();
        config.acks = parseAcks(opts.getOrDefault("acks", "all"));
        String sep = opts.get("key-separator");
        int sent = 0;
        try (TributaryProducer producer = new TributaryProducer(bootstrap, config)) {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                String key = null;
                String value = line;
                if (sep != null && line.contains(sep)) {
                    key = line.substring(0, line.indexOf(sep));
                    value = line.substring(line.indexOf(sep) + sep.length());
                }
                producer.send(topic, key, value).whenComplete((md, err) -> {
                    if (err != null) System.err.println("Send failed: " + err.getMessage());
                });
                sent++;
            }
            producer.flush();
        }
        System.err.printf("Produced %d message(s) to '%s'%n", sent, topic);
    }

    private static void consume(String bootstrap, Map<String, String> opts) throws Exception {
        String topic = require(opts, "topic");
        Integer partition = opts.containsKey("partition") ? Integer.valueOf(opts.get("partition")) : null;
        long max = Long.parseLong(opts.getOrDefault("max-messages", String.valueOf(Long.MAX_VALUE)));
        long idleTimeoutMs = Long.parseLong(opts.getOrDefault("idle-timeout-ms", "-1"));
        String group = opts.get("group");
        try (TributaryConsumer consumer = new TributaryConsumer(bootstrap, group, topic, partition, !opts.containsKey("from-latest"))) {
            consumer.initialize();
            long count = 0;
            long lastMessageAt = System.currentTimeMillis();
            while (count < max) {
                List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord r : records) {
                    System.out.printf("[%s-%d @%d] %s%s%n", r.topic(), r.partition(), r.offset(),
                            r.key() == null ? "" : r.keyAsString() + " => ", r.valueAsString());
                    if (++count >= max) break;
                }
                if (group != null) consumer.commitSync();
                if (!records.isEmpty()) {
                    lastMessageAt = System.currentTimeMillis();
                } else if (idleTimeoutMs >= 0 && System.currentTimeMillis() - lastMessageAt > idleTimeoutMs) {
                    break;
                }
            }
        }
    }

    static short parseAcks(String acks) {
        return switch (acks) {
            case "all", "-1" -> Protocol.ACKS_ALL;
            case "0" -> Protocol.ACKS_NONE;
            default -> Protocol.ACKS_LEADER;
        };
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    opts.put(key, args[++i]);
                } else {
                    opts.put(key, "true");
                }
            }
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String key) {
        String v = opts.get(key);
        if (v == null) {
            throw new IllegalArgumentException("Missing required option --" + key);
        }
        return v;
    }

    private static void usage() {
        System.err.println("""
                Usage: Cli <command> [--bootstrap host:port] [options]
                  create-topic --topic T [--partitions 3] [--replication-factor 3]
                  describe     [--topic T]
                  produce      --topic T [--acks all|1|0] [--key-separator :]   (reads lines from stdin)
                  consume      --topic T [--group G] [--partition P] [--from-latest] [--max-messages N] [--idle-timeout-ms MS]""");
    }
}
