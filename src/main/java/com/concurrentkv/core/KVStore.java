package com.concurrentkv.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Concurrent In-Memory Key-Value Store.
 * Coordinates thread-safe operations, LRU eviction, and TTL expiration
 * using a two-level locking hierarchy (ReentrantReadWriteLock for store access
 * and a dedicated ReentrantLock for LRU linked list pointer mutations).
 */
public class KVStore {

    private final int maxKeys;
    private final ConcurrentHashMap<String, Entry> map;
    private final DoublyLinkedList lruList;

    // Database read-write lock: allows concurrent readers while serializing structural writes.
    private final ReentrantReadWriteLock rwLock;
    // Dedicated lock for mutating LRU doubly linked list pointers.
    // Lock Hierarchy Rule: Always acquire rwLock BEFORE lruLock to prevent deadlocks.
    private final ReentrantLock lruLock;

    // Operational Statistics
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);
    private final AtomicLong expirations = new AtomicLong(0);
    private final AtomicLong totalCommands = new AtomicLong(0);
    private final long startTimeMs;

    public KVStore(int maxKeys) {
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be positive, given: " + maxKeys);
        }
        this.maxKeys = maxKeys;
        this.map = new ConcurrentHashMap<>(Math.min(maxKeys, 1024));
        this.lruList = new DoublyLinkedList();
        this.rwLock = new ReentrantReadWriteLock(true); // fair lock to prevent starvation
        this.lruLock = new ReentrantLock();
        this.startTimeMs = System.currentTimeMillis();
    }

    /**
     * Inserts or updates a key-value pair without expiration.
     */
    public boolean set(String key, String value) {
        return set(key, value, -1);
    }

    /**
     * Inserts or updates a key-value pair with TTL in seconds.
     * Thread-safe compound operation: updates map, LRU order, and handles capacity eviction.
     */
    public boolean set(String key, String value, long ttlSeconds) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        totalCommands.incrementAndGet();
        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            Entry existing = map.get(key);
            if (existing != null) {
                // Update existing entry
                existing.setValue(value);
                if (ttlSeconds > 0) {
                    existing.setTtlSeconds(ttlSeconds);
                } else if (ttlSeconds == 0) {
                    // Immediate expiration
                    removeEntryInternal(existing);
                    return true;
                } else {
                    // Retain or clear expiration
                    existing.removeTtl();
                }
                lruList.moveToHead(existing);
                return true;
            }

            // Key does not exist: check capacity and evict if full
            ensureCapacityBeforeInsert();

            // Create and insert new entry
            Entry newEntry = new Entry(key, value, ttlSeconds);
            map.put(key, newEntry);
            lruList.addToHead(newEntry);
            return true;
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the value associated with the key.
     * Updates LRU recency on a cache hit. Lazily evicts expired keys.
     */
    public String get(String key) {
        if (key == null) return null;
        totalCommands.incrementAndGet();

        // 1. Optimistic read with read lock
        rwLock.readLock().lock();
        Entry entry;
        boolean isExpired = false;
        try {
            entry = map.get(key);
            if (entry == null) {
                misses.incrementAndGet();
                return null;
            }
            if (entry.isExpired()) {
                isExpired = true;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        // 2. Handle lazy expiration if expired
        if (isExpired) {
            rwLock.writeLock().lock();
            lruLock.lock();
            try {
                // Re-verify under write lock
                Entry current = map.get(key);
                if (current != null && current.isExpired()) {
                    removeEntryInternal(current);
                    expirations.incrementAndGet();
                }
                misses.incrementAndGet();
                return null;
            } finally {
                lruLock.unlock();
                rwLock.writeLock().unlock();
            }
        }

        // 3. Cache hit: update LRU recency under lruLock
        lruLock.lock();
        try {
            // Verify entry is still connected to LRU list
            lruList.moveToHead(entry);
        } finally {
            lruLock.unlock();
        }

        hits.incrementAndGet();
        return entry.getValue();
    }

    /**
     * Deletes a key from the database.
     * Returns true if the key existed and was deleted, false otherwise.
     */
    public boolean del(String key) {
        if (key == null) return false;
        totalCommands.incrementAndGet();

        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            Entry entry = map.remove(key);
            if (entry != null) {
                lruList.remove(entry);
                return true;
            }
            return false;
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Sets or updates the TTL (in seconds) for an existing key.
     * Returns true if TTL was set, false if key does not exist or is expired.
     */
    public boolean ttl(String key, long seconds) {
        if (key == null) return false;
        totalCommands.incrementAndGet();

        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            Entry entry = map.get(key);
            if (entry == null) {
                return false;
            }
            if (entry.isExpired()) {
                removeEntryInternal(entry);
                expirations.incrementAndGet();
                return false;
            }

            if (seconds <= 0) {
                // Expire immediately
                removeEntryInternal(entry);
                expirations.incrementAndGet();
                return true;
            }

            entry.setTtlSeconds(seconds);
            return true;
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns remaining TTL in milliseconds.
     * Returns -2 if key does not exist or has expired.
     * Returns -1 if key exists but has no associated expiration.
     * Returns >= 0 remaining milliseconds otherwise.
     */
    public long pttl(String key) {
        if (key == null) return -2;
        totalCommands.incrementAndGet();

        rwLock.readLock().lock();
        try {
            Entry entry = map.get(key);
            if (entry == null) {
                return -2;
            }
            if (entry.isExpired()) {
                // Lazy cleanup in background/subsequent write
                return -2;
            }
            return entry.getRemainingTtlMs();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Checks if a key exists and has not expired.
     */
    public boolean exists(String key) {
        if (key == null) return false;
        totalCommands.incrementAndGet();

        rwLock.readLock().lock();
        try {
            Entry entry = map.get(key);
            if (entry == null) {
                return false;
            }
            if (entry.isExpired()) {
                return false;
            }
            return true;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns all active, non-expired keys currently stored in the database.
     */
    public List<String> keys() {
        totalCommands.incrementAndGet();
        rwLock.readLock().lock();
        try {
            List<String> liveKeys = new ArrayList<>();
            for (Map.Entry<String, Entry> item : map.entrySet()) {
                Entry e = item.getValue();
                if (e != null && !e.isExpired()) {
                    liveKeys.add(item.getKey());
                }
            }
            return liveKeys;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns the number of active, non-expired keys in the database.
     */
    public int dbsize() {
        totalCommands.incrementAndGet();
        rwLock.readLock().lock();
        try {
            int count = 0;
            for (Entry e : map.values()) {
                if (e != null && !e.isExpired()) {
                    count++;
                }
            }
            return count;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Removes all keys from the database.
     */
    public void flushall() {
        totalCommands.incrementAndGet();
        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            map.clear();
            lruList.clear();
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Samples and removes expired keys.
     * Called periodically by ExpirationManager.
     * Ensures we never delete a newer value or newer TTL.
     */
    public int purgeExpiredKeys(int maxSample) {
        if (maxSample <= 0) maxSample = 50;
        int purgedCount = 0;

        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            int sampled = 0;
            Iterator<Map.Entry<String, Entry>> iterator = map.entrySet().iterator();
            while (iterator.hasNext() && sampled < maxSample) {
                Map.Entry<String, Entry> item = iterator.next();
                sampled++;
                Entry entry = item.getValue();
                if (entry != null && entry.isExpired()) {
                    iterator.remove();
                    lruList.remove(entry);
                    purgedCount++;
                    expirations.incrementAndGet();
                }
            }
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
        return purgedCount;
    }

    /**
     * Ensures capacity is available before inserting a new key.
     * Must be called while holding both writeLock and lruLock.
     */
    private void ensureCapacityBeforeInsert() {
        if (map.size() < maxKeys) {
            return;
        }

        // 1. Opportunistically look for any expired entry from tail of LRU
        Entry current = lruList.peekTail();
        if (current != null && current.isExpired()) {
            removeEntryInternal(current);
            expirations.incrementAndGet();
            return;
        }

        // 2. If no expired entry immediately at tail, evict the least recently used live entry
        Entry evicted = lruList.removeTail();
        if (evicted != null) {
            map.remove(evicted.getKey());
            evictions.incrementAndGet();
        }
    }

    private void removeEntryInternal(Entry entry) {
        if (entry == null) return;
        map.remove(entry.getKey());
        lruList.remove(entry);
    }

    /**
     * Captures an atomic, consistent point-in-time snapshot of live entries.
     * Holding the readLock guarantees that concurrent writes do not mutate
     * the store while copying references.
     * The resulting list can be serialized to disk in a background executor
     * without blocking database reads or writes!
     */
    public List<Entry> captureSnapshotEntries() {
        rwLock.readLock().lock();
        lruLock.lock();
        try {
            // Traverse LRU from LRU to MRU so loading will recreate the exact access order
            List<Entry> allEntries = lruList.getAllLruToMru();
            List<Entry> snapshot = new ArrayList<>(allEntries.size());

            for (Entry entry : allEntries) {
                if (!entry.isExpired()) {
                    // Create an immutable snapshot entry copy
                    Entry copy = new Entry(
                            entry.getKey(),
                            entry.getValue(),
                            entry.getExpiryNanoTime(),
                            entry.getExpiryEpochMs()
                    );
                    snapshot.add(copy);
                }
            }
            return snapshot;
        } finally {
            lruLock.unlock();
            rwLock.readLock().unlock();
        }
    }

    /**
     * Restores state from a list of recovered entries.
     * Recreates the map and LRU structure while skipping any entries that have expired.
     */
    public int restoreFromSnapshot(List<Entry> loadedEntries) {
        rwLock.writeLock().lock();
        lruLock.lock();
        try {
            map.clear();
            lruList.clear();

            int restoredCount = 0;
            for (Entry entry : loadedEntries) {
                if (entry.hasExpiry() && entry.getExpiryEpochMs() <= System.currentTimeMillis()) {
                    // Already expired while server was offline
                    continue;
                }

                // If entry has a future expiry, recalculate monotonic nanoTime
                if (entry.hasExpiry()) {
                    long remainingMs = entry.getExpiryEpochMs() - System.currentTimeMillis();
                    entry.setTtlSeconds(Math.max(1, remainingMs / 1000));
                }

                map.put(entry.getKey(), entry);
                lruList.addToHead(entry);
                restoredCount++;

                if (restoredCount >= maxKeys) {
                    break;
                }
            }
            return restoredCount;
        } finally {
            lruLock.unlock();
            rwLock.writeLock().unlock();
        }
    }

    // Diagnostics and Statistics
    public int getMaxKeys() { return maxKeys; }
    public long getHits() { return hits.get(); }
    public long getMisses() { return misses.get(); }
    public long getEvictions() { return evictions.get(); }
    public long getExpirations() { return expirations.get(); }
    public long getTotalCommands() { return totalCommands.get(); }
    public long getUptimeSeconds() { return (System.currentTimeMillis() - startTimeMs) / 1000; }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("version", "1.0.0");
        stats.put("uptime_seconds", getUptimeSeconds());
        stats.put("current_keys", dbsize());
        stats.put("max_keys", maxKeys);
        stats.put("hits", hits.get());
        stats.put("misses", misses.get());
        stats.put("evictions", evictions.get());
        stats.put("expirations", expirations.get());
        stats.put("total_commands", totalCommands.get());
        return stats;
    }
}
