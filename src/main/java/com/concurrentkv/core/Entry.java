package com.concurrentkv.core;

import java.util.concurrent.TimeUnit;

/**
 * Represents a key-value entry stored in the database.
 * Also acts as a node in the LRU doubly linked list to minimize allocations.
 */
public class Entry {

    private final String key;
    private volatile String value;

    // Monotonic expiration timestamp: System.nanoTime() + nanos.
    // Value of -1 indicates no TTL (persistent key).
    private volatile long expiryNanoTime;

    // Wall-clock expiration timestamp (epoch milliseconds).
    // Used for object-independent snapshot persistence across server restarts.
    private volatile long expiryEpochMs;

    // Doubly linked list pointers for LRU tracking.
    // Guarded by the database LRU lock.
    Entry prev;
    Entry next;

    public Entry(String key, String value) {
        this(key, value, -1, -1);
    }

    public Entry(String key, String value, long ttlSeconds) {
        this.key = key;
        this.value = value;
        if (ttlSeconds > 0) {
            setTtlSeconds(ttlSeconds);
        } else {
            this.expiryNanoTime = -1;
            this.expiryEpochMs = -1;
        }
    }

    public Entry(String key, String value, long expiryNanoTime, long expiryEpochMs) {
        this.key = key;
        this.value = value;
        this.expiryEpochMs = expiryEpochMs;
        if (expiryNanoTime != -1) {
            this.expiryNanoTime = expiryNanoTime;
        } else if (expiryEpochMs != -1) {
            long deltaMs = expiryEpochMs - System.currentTimeMillis();
            if (deltaMs <= 0) {
                this.expiryNanoTime = System.nanoTime() - 1; // Mark expired in monotonic time
            } else {
                this.expiryNanoTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deltaMs);
            }
        } else {
            this.expiryNanoTime = -1;
        }
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean hasExpiry() {
        return expiryNanoTime != -1 || expiryEpochMs != -1;
    }

    /**
     * Checks if the entry has expired using monotonic clock or epoch ms.
     * Uses subtraction to properly handle potential nanoTime overflow.
     */
    public boolean isExpired() {
        if (!hasExpiry()) {
            return false;
        }
        if (expiryNanoTime != -1) {
            return (System.nanoTime() - expiryNanoTime) >= 0;
        }
        return expiryEpochMs != -1 && System.currentTimeMillis() >= expiryEpochMs;
    }

    /**
     * Returns remaining TTL in milliseconds.
     * Returns -1 if no TTL is set.
     * Returns -2 if the entry is already expired.
     */
    public long getRemainingTtlMs() {
        if (expiryNanoTime == -1) {
            return -1;
        }
        long remainingNanos = expiryNanoTime - System.nanoTime();
        if (remainingNanos <= 0) {
            return -2;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    /**
     * Sets expiration given a TTL in seconds.
     */
    public void setTtlSeconds(long seconds) {
        if (seconds <= 0) {
            // Immediate expiration
            this.expiryNanoTime = System.nanoTime();
            this.expiryEpochMs = System.currentTimeMillis();
        } else {
            this.expiryNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            this.expiryEpochMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(seconds);
        }
    }

    /**
     * Sets expiration given an absolute wall-clock epoch millisecond timestamp.
     * Used when recovering entries from a snapshot.
     */
    public void setExpiryEpochMs(long epochMs) {
        this.expiryEpochMs = epochMs;
        long deltaMs = epochMs - System.currentTimeMillis();
        if (deltaMs <= 0) {
            this.expiryNanoTime = System.nanoTime(); // Already expired
        } else {
            this.expiryNanoTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deltaMs);
        }
    }

    public void removeTtl() {
        this.expiryNanoTime = -1;
        this.expiryEpochMs = -1;
    }

    public long getExpiryNanoTime() {
        return expiryNanoTime;
    }

    public long getExpiryEpochMs() {
        return expiryEpochMs;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", expired=" + isExpired() +
                ", remainingTtlMs=" + getRemainingTtlMs() +
                '}';
    }
}
