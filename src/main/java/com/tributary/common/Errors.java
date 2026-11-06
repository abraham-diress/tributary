package com.tributary.common;

/**
 * Error codes carried in every response. Retriable errors are ones a client can recover from by
 * refreshing metadata and trying again (e.g. a leader moved to another broker).
 */
public final class Errors {
    public static final short NONE = 0;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 1;
    public static final short NOT_LEADER = 2;
    public static final short OFFSET_OUT_OF_RANGE = 3;
    public static final short TOPIC_ALREADY_EXISTS = 4;
    public static final short NOT_CONTROLLER = 5;
    public static final short INVALID_REQUEST = 6;
    public static final short REQUEST_TIMED_OUT = 7;
    public static final short NOT_ENOUGH_REPLICAS = 8;
    public static final short INVALID_REPLICATION_FACTOR = 9;
    public static final short LEADER_NOT_AVAILABLE = 10;
    public static final short FENCED_LEADER_EPOCH = 11;
    public static final short CORRUPT_MESSAGE = 12;
    public static final short INVALID_TOPIC = 13;
    public static final short UNKNOWN_SERVER_ERROR = 99;

    private Errors() {
    }

    public static boolean isRetriable(short code) {
        return switch (code) {
            case UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER, NOT_CONTROLLER, LEADER_NOT_AVAILABLE,
                 NOT_ENOUGH_REPLICAS, FENCED_LEADER_EPOCH -> true;
            default -> false;
        };
    }

    public static String name(short code) {
        return switch (code) {
            case NONE -> "NONE";
            case UNKNOWN_TOPIC_OR_PARTITION -> "UNKNOWN_TOPIC_OR_PARTITION";
            case NOT_LEADER -> "NOT_LEADER";
            case OFFSET_OUT_OF_RANGE -> "OFFSET_OUT_OF_RANGE";
            case TOPIC_ALREADY_EXISTS -> "TOPIC_ALREADY_EXISTS";
            case NOT_CONTROLLER -> "NOT_CONTROLLER";
            case INVALID_REQUEST -> "INVALID_REQUEST";
            case REQUEST_TIMED_OUT -> "REQUEST_TIMED_OUT";
            case NOT_ENOUGH_REPLICAS -> "NOT_ENOUGH_REPLICAS";
            case INVALID_REPLICATION_FACTOR -> "INVALID_REPLICATION_FACTOR";
            case LEADER_NOT_AVAILABLE -> "LEADER_NOT_AVAILABLE";
            case FENCED_LEADER_EPOCH -> "FENCED_LEADER_EPOCH";
            case CORRUPT_MESSAGE -> "CORRUPT_MESSAGE";
            case INVALID_TOPIC -> "INVALID_TOPIC";
            default -> "UNKNOWN_SERVER_ERROR";
        };
    }
}
