package com.concurrentkv.protocol;

/**
 * Supported database commands in ConcurrentKVStore.
 */
public enum CommandType {
    SET,
    GET,
    DEL,
    TTL,
    PTTL,
    EXISTS,
    KEYS,
    DBSIZE,
    FLUSHALL,
    PING,
    SAVE,
    INFO,
    QUIT;

    public static CommandType fromString(String name) {
        if (name == null) return null;
        try {
            return CommandType.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
