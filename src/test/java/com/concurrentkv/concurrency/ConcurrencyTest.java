package com.concurrentkv.concurrency;

import com.concurrentkv.core.Entry;
import com.concurrentkv.core.KVStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Thread Safety and Concurrency Tests")
public class ConcurrencyTest {

    @Test
    @DisplayName("Multiple threads concurrently reading and writing without corruption")
    void testConcurrentReadsAndWrites() throws InterruptedException {
        int capacity = 5000;
        KVStore store = new KVStore(capacity);
        int threadCount = 20;
        int operationsPerThread = 250;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // Synchronized starting gun

                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "thread-" + threadId + "-key-" + i;
                        String val = "value-" + i;
                        store.set(key, val);

                        String retrieved = store.get(key);
                        if (!val.equals(retrieved)) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await(5, TimeUnit.SECONDS);
        startLatch.countDown(); // Fire all threads at once

        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(completed, "All threads should complete in time");
        assertEquals(0, errorCount.get(), "No read/write errors should occur under concurrency");
        assertEquals(threadCount * operationsPerThread, store.dbsize());
    }

    @Test
    @DisplayName("Concurrent evictions strictly maintain maxKeys capacity limit")
    void testConcurrentEvictionsRespectCapacity() throws InterruptedException {
        int maxCapacity = 50;
        KVStore store = new KVStore(maxCapacity);
        int threadCount = 10;
        int keysPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < keysPerThread; i++) {
                        store.set("k-" + threadId + "-" + i, "val-" + i);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(store.dbsize() <= maxCapacity, 
                "Size must never exceed maxCapacity! Actual size: " + store.dbsize());
        assertTrue(store.getEvictions() > 0, "Evictions should have occurred");
    }

    @Test
    @DisplayName("Snapshot capture remains consistent during concurrent writes")
    void testSnapshotConsistencyDuringConcurrentWrites() throws InterruptedException {
        KVStore store = new KVStore(1000);
        int writerThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(writerThreads);
        AtomicBoolean keepWriting = new AtomicBoolean(true);

        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int i = 0;
                while (keepWriting.get()) {
                    store.set("key-" + threadId + "-" + (i % 50), "val-" + i);
                    i++;
                }
            });
        }

        // Let writers produce data
        Thread.sleep(100);

        // Capture snapshot while writers are actively running
        List<Entry> snapshot = store.captureSnapshotEntries();
        keepWriting.set(false);

        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.SECONDS);

        assertNotNull(snapshot);
        assertFalse(snapshot.isEmpty());
        // Verify every entry in the snapshot is non-null with valid key and value
        for (Entry e : snapshot) {
            assertNotNull(e.getKey());
            assertNotNull(e.getValue());
        }
    }
}
