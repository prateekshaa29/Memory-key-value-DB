package com.concurrentkv.server;

import com.concurrentkv.core.KVStore;
import com.concurrentkv.persistence.SnapshotManager;
import com.concurrentkv.protocol.Command;
import com.concurrentkv.protocol.CommandParser;
import com.concurrentkv.protocol.Response;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles communication with an individual TCP client.
 * Reads commands line-by-line, executes them against KVStore,
 * and transmits formatted protocol responses back.
 */
public class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final KVStore kvStore;
    private final SnapshotManager snapshotManager;
    private final KVServer server;
    private final CommandParser parser;
    private final AtomicInteger activeConnectionsCounter;

    public ClientHandler(Socket clientSocket,
                         KVStore kvStore,
                         SnapshotManager snapshotManager,
                         KVServer server,
                         int maxLineLength,
                         AtomicInteger activeConnectionsCounter) {
        this.clientSocket = clientSocket;
        this.kvStore = kvStore;
        this.snapshotManager = snapshotManager;
        this.server = server;
        this.parser = new CommandParser(maxLineLength);
        this.activeConnectionsCounter = activeConnectionsCounter;
    }

    @Override
    public void run() {
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String responseStr;
                boolean shouldClose = false;

                try {
                    Command cmd = parser.parse(line);
                    if (cmd == null) {
                        continue;
                    }

                    switch (cmd.getType()) {
                        case SET:
                            String key = cmd.getArg(0);
                            String value = cmd.getArg(1);
                            long ttlSec = cmd.getArgCount() == 3 ? Long.parseLong(cmd.getArg(2)) : -1;
                            kvStore.set(key, value, ttlSec);
                            responseStr = Response.ok();
                            break;

                        case GET:
                            String val = kvStore.get(cmd.getArg(0));
                            responseStr = Response.value(val);
                            break;

                        case DEL:
                            boolean deleted = kvStore.del(cmd.getArg(0));
                            responseStr = Response.integer(deleted ? 1 : 0);
                            break;

                        case TTL:
                            long seconds = Long.parseLong(cmd.getArg(1));
                            boolean ttlSet = kvStore.ttl(cmd.getArg(0), seconds);
                            responseStr = Response.integer(ttlSet ? 1 : 0);
                            break;

                        case PTTL:
                            long remainingMs = kvStore.pttl(cmd.getArg(0));
                            responseStr = Response.integer(remainingMs);
                            break;

                        case EXISTS:
                            boolean exists = kvStore.exists(cmd.getArg(0));
                            responseStr = Response.integer(exists ? 1 : 0);
                            break;

                        case KEYS:
                            List<String> keys = kvStore.keys();
                            responseStr = Response.keys(keys);
                            break;

                        case DBSIZE:
                            int count = kvStore.dbsize();
                            responseStr = Response.integer(count);
                            break;

                        case FLUSHALL:
                            kvStore.flushall();
                            responseStr = Response.ok();
                            break;

                        case PING:
                            if (cmd.getArgCount() == 0) {
                                responseStr = Response.pong();
                            } else {
                                responseStr = Response.pong(cmd.getArg(0));
                            }
                            break;

                        case SAVE:
                            try {
                                snapshotManager.saveSync();
                                responseStr = Response.ok("snapshot saved");
                            } catch (Exception e) {
                                responseStr = Response.error("failed to save snapshot: " + e.getMessage());
                            }
                            break;

                        case INFO:
                            Map<String, Object> stats = kvStore.getStats();
                            stats.put("connected_clients", server != null ? server.getActiveClients() : 1);
                            stats.put("snapshots_saved", snapshotManager != null ? snapshotManager.getSuccessfulSnapshots() : 0);
                            responseStr = Response.info(stats);
                            break;

                        case QUIT:
                            responseStr = Response.ok("bye");
                            shouldClose = true;
                            break;

                        default:
                            responseStr = Response.error("unknown command");
                            break;
                    }
                } catch (IllegalArgumentException e) {
                    responseStr = Response.error(e.getMessage());
                } catch (Exception e) {
                    responseStr = Response.error("internal server error: " + e.getMessage());
                }

                writer.write(responseStr);
                writer.flush();

                if (shouldClose) {
                    break;
                }
            }
        } catch (SocketException ignored) {
            // Normal client disconnect or socket closed
        } catch (IOException e) {
            System.err.println("[WARN] I/O error handling client " + clientAddress + ": " + e.getMessage());
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException ignored) {}
            if (activeConnectionsCounter != null) {
                activeConnectionsCounter.decrementAndGet();
            }
        }
    }
}
