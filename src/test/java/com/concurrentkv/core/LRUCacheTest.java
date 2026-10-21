package com.concurrentkv.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LRU Cache Eviction and Recency Test")
public class LRUCacheTest {

    @Test
    @DisplayName("Least recently used key is evicted when capacity is exceeded")
    void testBasicLruEviction() {
        KVStore cache = new KVStore(3);

        cache.set("k1", "v1");
        cache.set("k2", "v2");
        cache.set("k3", "v3");

        assertEquals(3, cache.dbsize());
        assertEquals(0, cache.getEvictions());

        // Inserting 4th item when capacity is 3 should evict k1 (least recently used)
        cache.set("k4", "v4");

        assertEquals(3, cache.dbsize());
        assertEquals(1, cache.getEvictions());

        assertNull(cache.get("k1"), "k1 should have been evicted");
        assertEquals("v2", cache.get("k2"));
        assertEquals("v3", cache.get("k3"));
        assertEquals("v4", cache.get("k4"));
    }

    @Test
    @DisplayName("GET updates recency and prevents eviction")
    void testGetUpdatesRecency() {
        KVStore cache = new KVStore(3);

        cache.set("k1", "v1");
        cache.set("k2", "v2");
        cache.set("k3", "v3");

        // Access k1 -> moves k1 to most recently used. LRU is now k2!
        assertEquals("v1", cache.get("k1"));

        // Insert k4 -> k2 should be evicted, NOT k1!
        cache.set("k4", "v4");

        assertEquals("v1", cache.get("k1"), "k1 should be retained because of recent GET");
        assertNull(cache.get("k2"), "k2 should be evicted as the least recently used");
        assertEquals("v3", cache.get("k3"));
        assertEquals("v4", cache.get("k4"));
    }

    @Test
    @DisplayName("SET on existing key updates recency and prevents eviction")
    void testSetUpdatesRecency() {
        KVStore cache = new KVStore(3);

        cache.set("k1", "v1");
        cache.set("k2", "v2");
        cache.set("k3", "v3");

        // Update k1 -> moves k1 to most recently used
        cache.set("k1", "v1_updated");

        // Insert k4 -> k2 should be evicted
        cache.set("k4", "v4");

        assertEquals("v1_updated", cache.get("k1"));
        assertNull(cache.get("k2"), "k2 should be evicted");
        assertEquals("v3", cache.get("k3"));
        assertEquals("v4", cache.get("k4"));
    }

    @Test
    @DisplayName("Expired entries at tail are purged before live entries")
    void testExpiredEntryEvictionPriority() throws InterruptedException {
        KVStore cache = new KVStore(2);

        // k1 expires in 1 second
        cache.set("k1", "v1", 1);
        cache.set("k2", "v2"); // persistent

        // Sleep to let k1 expire
        Thread.sleep(1100);

        // Insert k3 -> capacity is 2. k1 is already expired, so k1 should be removed instead of evicting live k2!
        cache.set("k3", "v3");

        assertNull(cache.get("k1"), "k1 was expired and removed");
        assertEquals("v2", cache.get("k2"), "Live entry k2 should NOT be evicted");
        assertEquals("v3", cache.get("k3"));
    }
}
