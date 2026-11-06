package com.tributary.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Holds information about a broker in the Tributary cluster. */
public final class BrokerInfo {
    private static final Pattern JSON = Pattern.compile("\"host\"\\s*:\\s*\"([^\"]+)\".*\"port\"\\s*:\\s*(\\d+)");

    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /** Serialized form stored in the broker's ZooKeeper registration node. */
    public String toJson() {
        return "{\"host\":\"" + host + "\",\"port\":" + port + "}";
    }

    public static BrokerInfo fromJson(int id, String json) {
        Matcher m = JSON.matcher(json);
        if (!m.find()) {
            throw new IllegalArgumentException("Invalid broker registration: " + json);
        }
        return new BrokerInfo(id, m.group(1), Integer.parseInt(m.group(2)));
    }

    @Override
    public String toString() {
        return "BrokerInfo{id=" + id + ", host='" + host + "', port=" + port + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BrokerInfo other)) return false;
        return id == other.id && port == other.port && host.equals(other.host);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}
