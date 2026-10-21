package com.concurrentkv;

import com.concurrentkv.client.KVClientCLI;
import com.concurrentkv.config.ServerConfig;
import com.concurrentkv.server.KVServer;

import java.util.Arrays;

/**
 * Main Entry Point for ConcurrentKVStore.
 * Launches either the TCP Server or the interactive CLI Client.
 */
public class Main {

    public static void main(String[] args) {
        if (args.length > 0 && (args[0].equalsIgnoreCase("client") || args[0].equalsIgnoreCase("cli"))) {
            // Forward remaining args to CLI client
            String[] clientArgs = Arrays.copyOfRange(args, 1, args.length);
            KVClientCLI.main(clientArgs);
            return;
        }

        // Start Server by default
        ServerConfig config = ServerConfig.load(null, args);
        KVServer server = new KVServer(config);

        // Register graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[INFO] JVM shutdown initiated. Stopping ConcurrentKVStore server...");
            server.stop();
        }, "kv-shutdown-hook"));

        try {
            server.start();

            // Keep main thread alive while server runs
            Thread.currentThread().join();

        } catch (InterruptedException e) {
            System.out.println("[INFO] Server main thread interrupted. Exiting.");
            server.stop();
        } catch (Exception e) {
            System.err.println("[FATAL] Server terminated with error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
