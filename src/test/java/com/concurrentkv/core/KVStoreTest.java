package com.concurrentkv.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("KVStore Core Operations Test")
public class KVStoreTest {

    private KVStore store;

    @BeforeEach
    void setUp() {
        store = new KVStore(100);
    }

    @Test
    @DisplayName("SET and GET operations return correct values")
    void testSetAndGet() {
        assertTrue(store.set("user:1", "Alice"));
        assertEquals("Alice", store.get("user:1"));

        // Overwrite
        assertTrue(store.set("user:1", "Bob"));
        assertEquals("Bob", store.get("user:1"));
    }

    @Test
    @DisplayName("GET missing key returns null")
    void testGetMissingKey() {
        assertNull(store.get("nonexistent"));
        assertEquals(0, store.getHits());
        assertEquals(1, store.getMisses());
    }

    @Test
    @DisplayName("DEL removes existing key and returns correct status")
    void testDel() {
        store.set("key1", "val1");
        assertTrue(store.exists("key1"));

        assertTrue(store.del("key1"));
        assertNull(store.get("key1"));
        assertFalse(store.exists("key1"));

        // Second delete should return false
        assertFalse(store.del("key1"));
    }

    @Test
    @DisplayName("EXISTS checks presence accurately")
    void testExists() {
        assertFalse(store.exists("alpha"));
        store.set("alpha", "123");
        assertTrue(store.exists("alpha"));
    }

    @Test
    @DisplayName("KEYS lists all active keys")
    void testKeys() {
        store.set("k1", "v1");
        store.set("k2", "v2");
        store.set("k3", "v3");

        List<String> keys = store.keys();
        assertEquals(3, keys.size());
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
        assertTrue(keys.contains("k3"));
    }

    @Test
    @DisplayName("DBSIZE reflects active count and decrements on DEL")
    void testDbSize() {
        assertEquals(0, store.dbsize());
        store.set("a", "1");
        store.set("b", "2");
        assertEquals(2, store.dbsize());

        store.del("a");
        assertEquals(1, store.dbsize());
    }

    @Test
    @DisplayName("FLUSHALL clears all keys")
    void testFlushAll() {
        store.set("a", "1");
        store.set("b", "2");
        store.set("c", "3");
        assertEquals(3, store.dbsize());

        store.flushall();
        assertEquals(0, store.dbsize());
        assertTrue(store.keys().isEmpty());
        assertNull(store.get("a"));
    }

    @Test
    @DisplayName("Null arguments throw IllegalArgumentException")
    void testNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> store.set(null, "val"));
        assertThrows(IllegalArgumentException.class, () -> store.set("key", null));
        assertNull(store.get(null));
        assertFalse(store.del(null));
    }
}
