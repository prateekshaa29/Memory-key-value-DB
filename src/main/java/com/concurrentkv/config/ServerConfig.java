package com.concurrentkv.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Server and Storage Configuration for ConcurrentKVStore.
 * Loads defaults, reads from application.properties, and allows command-line overrides.
 */
public class ServerConfig {

    private String host = "127.0.0.1";
    private int port = 6379;
    private int maxConnections = 50;
    private int workerThreads = 16;
    private int maxKeys = 1000;
    private String dataDir = "./data";
    private String snapshotFilename = "snapshot.ckv";
    private int snapshotIntervalSeconds = 60;
    private int ttlCleanupIntervalMs = 200;
    private int maxLineLength = 65536;

    public ServerConfig() {
        loadFromClasspath();
    }

    public static ServerConfig loadDefault() {
        return new ServerConfig();
    }

    public static ServerConfig load(String configFilePath, String[] args) {
        ServerConfig config = new ServerConfig();
        if (configFilePath != null && !configFilePath.trim().isEmpty()) {
            config.loadFromFile(new File(configFilePath));
        }
        if (args != null) {
            config.parseArgs(args);
        }
        return config;
    }

    private void loadFromClasspath() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                applyProperties(props);
            }
        } catch (IOException ignored) {
            // Fall back to built-in default values
        }
    }

    public void loadFromFile(File file) {
        if (!file.exists()) {
            System.err.println("[WARN] Config file " + file.getAbsolutePath() + " not found. Using defaults.");
            return;
        }
        try (InputStream in = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(in);
            applyProperties(props);
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to load config from " + file.getAbsolutePath() + ": " + e.getMessage());
        }
    }

    private void applyProperties(Properties props) {
        if (props.containsKey("server.host")) {
            this.host = props.getProperty("server.host").trim();
        }
        if (props.containsKey("server.port")) {
            this.port = Integer.parseInt(props.getProperty("server.port").trim());
        }
        if (props.containsKey("server.max-connections")) {
            this.maxConnections = Integer.parseInt(props.getProperty("server.max-connections").trim());
        }
        if (props.containsKey("server.worker-threads")) {
            this.workerThreads = Integer.parseInt(props.getProperty("server.worker-threads").trim());
        }
        if (props.containsKey("database.max-keys")) {
            this.maxKeys = Integer.parseInt(props.getProperty("database.max-keys").trim());
        }
        if (props.containsKey("storage.data-dir")) {
            this.dataDir = props.getProperty("storage.data-dir").trim();
        }
        if (props.containsKey("storage.snapshot-filename")) {
            this.snapshotFilename = props.getProperty("storage.snapshot-filename").trim();
        }
        if (props.containsKey("storage.snapshot-interval-seconds")) {
            this.snapshotIntervalSeconds = Integer.parseInt(props.getProperty("storage.snapshot-interval-seconds").trim());
        }
        if (props.containsKey("ttl.cleanup-interval-ms")) {
            this.ttlCleanupIntervalMs = Integer.parseInt(props.getProperty("ttl.cleanup-interval-ms").trim());
        }
        if (props.containsKey("protocol.max-line-length")) {
            this.maxLineLength = Integer.parseInt(props.getProperty("protocol.max-line-length").trim());
        }
    }

    public void parseArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                this.port = Integer.parseInt(arg.substring("--port=".length()).trim());
            } else if (arg.startsWith("--host=")) {
                this.host = arg.substring("--host=".length()).trim();
            } else if (arg.startsWith("--max-keys=")) {
                this.maxKeys = Integer.parseInt(arg.substring("--max-keys=".length()).trim());
            } else if (arg.startsWith("--data-dir=")) {
                this.dataDir = arg.substring("--data-dir=".length()).trim();
            } else if (arg.startsWith("--snapshot-interval=")) {
                this.snapshotIntervalSeconds = Integer.parseInt(arg.substring("--snapshot-interval=".length()).trim());
            } else if (arg.startsWith("--config=")) {
                loadFromFile(new File(arg.substring("--config=".length()).trim()));
            }
        }
    }

    // Getters and Setters
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public int getMaxConnections() { return maxConnections; }
    public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }

    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }

    public int getMaxKeys() { return maxKeys; }
    public void setMaxKeys(int maxKeys) { this.maxKeys = maxKeys; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public String getSnapshotFilename() { return snapshotFilename; }
    public void setSnapshotFilename(String snapshotFilename) { this.snapshotFilename = snapshotFilename; }

    public int getSnapshotIntervalSeconds() { return snapshotIntervalSeconds; }
    public void setSnapshotIntervalSeconds(int snapshotIntervalSeconds) { this.snapshotIntervalSeconds = snapshotIntervalSeconds; }

    public int getTtlCleanupIntervalMs() { return ttlCleanupIntervalMs; }
    public void setTtlCleanupIntervalMs(int ttlCleanupIntervalMs) { this.ttlCleanupIntervalMs = ttlCleanupIntervalMs; }

    public int getMaxLineLength() { return maxLineLength; }
    public void setMaxLineLength(int maxLineLength) { this.maxLineLength = maxLineLength; }
}
