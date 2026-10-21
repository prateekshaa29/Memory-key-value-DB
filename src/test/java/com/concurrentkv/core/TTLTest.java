package com.concurrentkv.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TTL Expiration Tests")
public class TTLTest {

    @Test
    @DisplayName("Key with TTL returns correct PTTL and expires as expected")
    void testTtlAndPttl() throws InterruptedException {
        KVStore store = new KVStore(50);

        store.set("session", "abc", 2); // 2 seconds

        long pttl = store.pttl("session");
        assertTrue(pttl > 0 && pttl <= 2000, "PTTL should be between 0 and 2000 ms, actual: " + pttl);
        assertEquals("abc", store.get("session"));

        // Wait for expiration
        Thread.sleep(2100);

        // Monotonic check immediately marks it expired
        assertNull(store.get("session"), "Expired key must return null on GET");
        assertEquals(-2, store.pttl("session"), "Expired key returns -2 on PTTL");
        assertFalse(store.exists("session"));
    }

    @Test
    @DisplayName("Key without TTL returns -1 on PTTL")
    void testPersistentKeyPttl() {
        KVStore store = new KVStore(50);
        store.set("permanent", "data");

        assertEquals(-1, store.pttl("permanent"));
        assertEquals(-2, store.pttl("missing_key"));
    }

    @Test
    @DisplayName("Updating TTL on existing key changes its expiration")
    void testUpdateTtlOnExistingKey() throws InterruptedException {
        KVStore store = new KVStore(50);
        store.set("temp", "val");

        assertEquals(-1, store.pttl("temp"));

        // Add TTL of 2 seconds
        assertTrue(store.ttl("temp", 2));
        assertTrue(store.pttl("temp") > 0);

        // Update TTL to 10 seconds
        assertTrue(store.ttl("temp", 10));
        assertTrue(store.pttl("temp") > 2000);

        // Setting TTL on non-existent key returns false
        assertFalse(store.ttl("no_such_key", 5));
    }

    @Test
    @DisplayName("Setting TTL to zero expires key immediately")
    void testTtlZeroExpiresImmediately() {
        KVStore store = new KVStore(50);
        store.set("now", "gone");

        assertTrue(store.ttl("now", 0));
        assertNull(store.get("now"));
        assertFalse(store.exists("now"));
    }

    @Test
    @DisplayName("Expired keys do not count toward DBSIZE or KEYS")
    void testExpiredKeysExcludedFromSizeAndKeys() throws InterruptedException {
        KVStore store = new KVStore(50);
        store.set("k1", "v1", 1);
        store.set("k2", "v2"); // permanent

        assertEquals(2, store.dbsize());
        assertTrue(store.keys().contains("k1"));

        Thread.sleep(1100);

        assertEquals(1, store.dbsize(), "Expired key should not count toward DBSIZE");
        assertFalse(store.keys().contains("k1"), "Expired key should not be listed in KEYS");
        assertTrue(store.keys().contains("k2"));
    }

    @Test
    @DisplayName("Background ExpirationManager removes expired keys without client GET")
    void testBackgroundExpirationManager() throws InterruptedException {
        KVStore store = new KVStore(50);
        ExpirationManager manager = new ExpirationManager(store, 100);
        manager.start();

        try {
            store.set("ephemeral", "123", 1);
            assertEquals(1, store.dbsize());

            // Wait 1.3 seconds for TTL to expire and background cleaner to run
            Thread.sleep(1300);

            // Active cleanup should have removed it
            assertTrue(store.getExpirations() >= 1, "Background cleaner should increment expiration counter");
            assertEquals(0, store.dbsize());
        } finally {
            manager.stop();
        }
    }
}
