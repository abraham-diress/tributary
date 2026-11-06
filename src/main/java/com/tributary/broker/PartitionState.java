package com.tributary.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The authoritative state of one partition, stored as JSON in ZooKeeper at
 * {@code /topics/<topic>/partitions/<id>/state}. Every leader change bumps {@code leaderEpoch},
 * which lets brokers ignore stale updates and fence fetches from out-of-date followers.
 * {@code leader == -1} means the partition is offline.
 */
public record PartitionState(int leader, int leaderEpoch, List<Integer> replicas, List<Integer> isr) {
    private static final Pattern LEADER = Pattern.compile("\"leader\"\\s*:\\s*(-?\\d+)");
    private static final Pattern EPOCH = Pattern.compile("\"leader_epoch\"\\s*:\\s*(\\d+)");
    private static final Pattern REPLICAS = Pattern.compile("\"replicas\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern ISR = Pattern.compile("\"isr\"\\s*:\\s*\\[([^\\]]*)\\]");

    public PartitionState {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
    }

    public PartitionState withIsr(List<Integer> newIsr) {
        return new PartitionState(leader, leaderEpoch, replicas, newIsr);
    }

    public String toJson() {
        return "{\"leader\":" + leader + ",\"leader_epoch\":" + leaderEpoch
                + ",\"replicas\":" + replicas.toString().replace(" ", "")
                + ",\"isr\":" + isr.toString().replace(" ", "") + "}";
    }

    public static PartitionState fromJson(String json) {
        return new PartitionState(
                Integer.parseInt(find(LEADER, json)),
                Integer.parseInt(find(EPOCH, json)),
                parseList(find(REPLICAS, json)),
                parseList(find(ISR, json)));
    }

    private static String find(Pattern p, String json) {
        Matcher m = p.matcher(json);
        if (!m.find()) {
            throw new IllegalArgumentException("Invalid partition state: " + json);
        }
        return m.group(1);
    }

    private static List<Integer> parseList(String csv) {
        List<Integer> out = new ArrayList<>();
        for (String s : csv.split(",")) {
            if (!s.isBlank()) {
                out.add(Integer.parseInt(s.trim()));
            }
        }
        return out;
    }
}
