package com.concurrentkv.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Interactive Command Line Interface (CLI) for ConcurrentKVStore.
 * Allows users to interact with the database server using Redis-like commands.
 */
public class KVClientCLI {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 6379;

        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[1]);
                System.exit(1);
            }
        }

        printBanner(host, port);

        try (KVClient client = new KVClient(host, port);
             BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            System.out.println("Connected to ConcurrentKVStore at " + host + ":" + port);
            System.out.println("Type 'help' for available commands, 'quit' or 'exit' to disconnect.\n");

            while (true) {
                System.out.print("kv-cli [" + host + ":" + port + "]> ");
                String line = consoleReader.readLine();
                if (line == null) {
                    break;
                }

                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    System.out.println("Goodbye!");
                    break;
                }

                if (line.equalsIgnoreCase("help")) {
                    printHelp();
                    continue;
                }

                try {
                    String response = client.sendRawCommand(line);
                    System.out.println(response);
                } catch (Exception e) {
                    System.err.println("(error) " + e.getMessage());
                    break;
                }
            }

        } catch (Exception e) {
            System.err.println("Could not connect to ConcurrentKVStore at " + host + ":" + port + " -> " + e.getMessage());
        }
    }

    private static void printBanner(String host, int port) {
        System.out.println("==================================================================");
        System.out.println("  ConcurrentKVStore Interactive Client (CLI)");
        System.out.println("  Target: " + host + ":" + port);
        System.out.println("==================================================================");
    }

    private static void printHelp() {
        System.out.println("\nAvailable Commands:");
        System.out.println("  SET key value [ttl]    - Store a key-value pair with optional TTL (seconds)");
        System.out.println("  GET key                - Retrieve value by key");
        System.out.println("  DEL key                - Delete a key");
        System.out.println("  TTL key seconds        - Update key expiration in seconds");
        System.out.println("  PTTL key               - View remaining TTL in milliseconds");
        System.out.println("  EXISTS key             - Check if a key exists");
        System.out.println("  KEYS                   - List all active keys");
        System.out.println("  DBSIZE                 - Number of active keys");
        System.out.println("  FLUSHALL               - Delete all stored keys");
        System.out.println("  SAVE                   - Trigger immediate snapshot persistence");
        System.out.println("  PING [msg]             - Test server connectivity");
        System.out.println("  INFO                   - Display database and system metrics");
        System.out.println("  QUIT / EXIT            - Disconnect and exit CLI\n");
    }
}
