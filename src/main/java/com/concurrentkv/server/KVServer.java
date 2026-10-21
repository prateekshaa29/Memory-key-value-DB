package com.concurrentkv.server;

import com.concurrentkv.config.ServerConfig;
import com.concurrentkv.core.ExpirationManager;
import com.concurrentkv.core.KVStore;
import com.concurrentkv.persistence.SnapshotException;
import com.concurrentkv.persistence.SnapshotManager;
import com.concurrentkv.protocol.Response;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance, concurrent TCP Key-Value Server.
 * Coordinates bounded client worker pools, memory storage, LRU caching,
 * TTL expiration, and snapshot persistence.
 */
public class KVServer {

    private final ServerConfig config;
    private final KVStore kvStore;
    private final ExpirationManager expirationManager;
    private final SnapshotManager snapshotManager;

    private ServerSocket serverSocket;
    private ExecutorService clientWorkerPool;
    private final AtomicInteger activeClients = new AtomicInteger(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Thread acceptThread;

    public KVServer(ServerConfig config) {
        this.config = config;
        this.kvStore = new KVStore(config.getMaxKeys());
        this.expirationManager = new ExpirationManager(kvStore, config.getTtlCleanupIntervalMs());
        this.snapshotManager = new SnapshotManager(
                kvStore,
                config.getDataDir(),
                config.getSnapshotFilename(),
                config.getSnapshotIntervalSeconds()
        );
    }

    /**
     * Initializes components, restores snapshot, and starts accepting client connections.
     */
    public synchronized void start() throws IOException {
        if (!isRunning.compareAndSet(false, true)) {
            System.out.println("[WARN] Server is already running.");
            return;
        }

        System.out.println("=================================================");
        System.out.println("  ConcurrentKVStore Server starting up...");
        System.out.println("  Listening on: " + config.getHost() + ":" + config.getPort());
        System.out.println("  Max Keys (LRU Capacity): " + config.getMaxKeys());
        System.out.println("  Max Connections: " + config.getMaxConnections());
        System.out.println("  Worker Threads: " + config.getWorkerThreads());
        System.out.println("=================================================");

        // 1. Recover from snapshot if available
        try {
            int restoredCount = snapshotManager.loadSnapshot();
            if (restoredCount > 0) {
                System.out.println("[INFO] Restored " + restoredCount + " keys from persistent snapshot.");
            }
        } catch (SnapshotException e) {
            System.err.println("[WARN] Snapshot recovery failed: " + e.getMessage() + ". Starting with empty database.");
        }

        // 2. Start background tasks
        expirationManager.start();
        snapshotManager.start();

        // 3. Initialize bounded client worker thread pool
        this.clientWorkerPool = new ThreadPoolExecutor(
                config.getWorkerThreads(),
                config.getWorkerThreads() * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.getMaxConnections()),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "kv-client-worker-" + counter.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy()
        );

        // 4. Bind ServerSocket
        InetAddress bindAddr = InetAddress.getByName(config.getHost());
        this.serverSocket = new ServerSocket(config.getPort(), config.getMaxConnections(), bindAddr);

        // 5. Start connection accept loop in background thread
        this.acceptThread = new Thread(this::listenForClients, "kv-accept-listener");
        this.acceptThread.start();

        System.out.println("[INFO] Server is ready to accept connections on " + config.getHost() + ":" + config.getPort());
    }

    private void listenForClients() {
        while (isRunning.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();

                // Enforce connection limit
                if (activeClients.get() >= config.getMaxConnections()) {
                    rejectConnection(clientSocket, "ERR server connection limit reached\r\n");
                    continue;
                }

                activeClients.incrementAndGet();
                try {
                    clientWorkerPool.submit(new ClientHandler(
                            clientSocket,
                            kvStore,
                            snapshotManager,
                            this,
                            config.getMaxLineLength(),
                            activeClients
                    ));
                } catch (RejectedExecutionException rex) {
                    activeClients.decrementAndGet();
                    rejectConnection(clientSocket, "ERR server is currently overloaded\r\n");
                }

            } catch (SocketException e) {
                // ServerSocket was closed during shutdown
                break;
            } catch (IOException e) {
                if (isRunning.get()) {
                    System.err.println("[WARN] Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }

    private void rejectConnection(Socket socket, String message) {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            writer.write(message);
            writer.flush();
        } catch (IOException ignored) {}
        finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Gracefully stops the server: closes network listeners, saves snapshot to disk,
     * stops background managers, and waits for active client handlers to finish.
     */
    public synchronized void stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        System.out.println("[INFO] Initiating graceful server shutdown...");

        // 1. Close ServerSocket so no new connections are accepted
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.err.println("[WARN] Error closing server socket: " + e.getMessage());
            }
        }

        // 2. Persist final database state to disk
        try {
            System.out.println("[INFO] Saving final snapshot before shutdown...");
            snapshotManager.saveSync();
            System.out.println("[INFO] Final snapshot saved successfully.");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to save final snapshot during shutdown: " + e.getMessage());
        }

        // 3. Stop background managers
        expirationManager.stop();
        snapshotManager.shutdown();

        // 4. Shutdown client worker pool
        if (clientWorkerPool != null) {
            clientWorkerPool.shutdown();
            try {
                if (!clientWorkerPool.awaitTermination(3, TimeUnit.SECONDS)) {
                    clientWorkerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                clientWorkerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("[INFO] Server shutdown complete.");
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public int getActiveClients() {
        return activeClients.get();
    }

    public KVStore getKVStore() {
        return kvStore;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : config.getPort();
    }
}
