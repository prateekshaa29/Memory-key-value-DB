package com.concurrentkv.integration;

import com.concurrentkv.client.KVClient;
import com.concurrentkv.config.ServerConfig;
import com.concurrentkv.server.KVServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Live Demo Restart Persistence Verification")
public class LiveDemoVerificationTest {

    private File demoDataDir;
    private int demoPort = 6399;

    @BeforeEach
    void setUp() throws IOException {
        demoDataDir = Files.createTempDirectory("kv-demo-data").toFile();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (demoDataDir != null && demoDataDir.exists()) {
            Files.walk(demoDataDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Demonstrates save, server stop, server restart, and data restoration")
    void testSaveStopRestartAndRecover() throws Exception {
        ServerConfig config1 = new ServerConfig();
        config1.setHost("127.0.0.1");
        config1.setPort(demoPort);
        config1.setDataDir(demoDataDir.getAbsolutePath());
        config1.setSnapshotIntervalSeconds(0);

        // 1. Start Server 1
        KVServer server1 = new KVServer(config1);
        server1.start();

        // 2. Client sets data and saves snapshot
        try (KVClient client = new KVClient("127.0.0.1", demoPort)) {
            assertEquals("PONG", client.ping());
            assertTrue(client.set("student:name", "Prateekshaa"));
            assertTrue(client.set("student:role", "Senior Backend Engineer"));
            assertTrue(client.set("student:dream_company", "Google"));

            assertEquals("Prateekshaa", client.get("student:name"));
            assertEquals("Senior Backend Engineer", client.get("student:role"));

            // Persist snapshot to disk
            String saveResult = client.save();
            assertTrue(saveResult.startsWith("OK"));
        }

        // 3. Stop Server 1 gracefully
        server1.stop();
        assertFalse(server1.isRunning());

        // 4. Start Server 2 pointing to the exact same data directory
        ServerConfig config2 = new ServerConfig();
        config2.setHost("127.0.0.1");
        config2.setPort(demoPort);
        config2.setDataDir(demoDataDir.getAbsolutePath());
        config2.setSnapshotIntervalSeconds(0);

        KVServer server2 = new KVServer(config2);
        server2.start();

        // 5. Connect client to Server 2 and read back persisted data
        try (KVClient client2 = new KVClient("127.0.0.1", demoPort)) {
            assertEquals("Prateekshaa", client2.get("student:name"), "Data must persist across restarts!");
            assertEquals("Senior Backend Engineer", client2.get("student:role"));
            assertEquals("Google", client2.get("student:dream_company"));
            assertEquals(3, client2.dbsize());
        }

        // 6. Clean stop of Server 2
        server2.stop();
        assertFalse(server2.isRunning());
    }
}
