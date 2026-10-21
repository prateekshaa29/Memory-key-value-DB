package com.concurrentkv.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background Active TTL Expiration Manager.
 * Periodically samples and removes expired entries from the KVStore
 * without blocking concurrent user commands.
 */
public class ExpirationManager {

    private final KVStore kvStore;
    private final int intervalMs;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ExpirationManager(KVStore kvStore, int intervalMs) {
        this.kvStore = kvStore;
        this.intervalMs = Math.max(50, intervalMs);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "kv-ttl-cleaner-" + count.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * Starts the periodic background expiration task.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(this::runCleanupCycle, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Executes one cycle of sampling and removing expired keys.
     */
    void runCleanupCycle() {
        try {
            // Sample up to 100 keys per cycle to keep write lock duration negligible
            kvStore.purgeExpiredKeys(100);
        } catch (Throwable t) {
            System.err.println("[WARN] Unexpected error during TTL cleanup cycle: " + t.getMessage());
        }
    }

    /**
     * Gracefully stops the expiration cleaner.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }
}
