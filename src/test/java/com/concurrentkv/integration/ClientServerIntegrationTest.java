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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Client-Server TCP End-to-End Integration Tests")
public class ClientServerIntegrationTest {

    private KVServer server;
    private File tempDir;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("kv-integ-test").toFile();

        ServerConfig config = new ServerConfig();
        config.setHost("127.0.0.1");
        config.setPort(0); // Ephemeral port
        config.setDataDir(tempDir.getAbsolutePath());
        config.setSnapshotIntervalSeconds(0);
        config.setMaxConnections(50);
        config.setMaxKeys(500);

        server = new KVServer(config);
        server.start();
        port = server.getPort();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.stop();
        }
        if (tempDir != null && tempDir.exists()) {
            Files.walk(tempDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("End-to-End client operations over TCP socket")
    void testFullClientServerLifecycle() throws Exception {
        try (KVClient client = new KVClient("127.0.0.1", port)) {
            // PING
            assertEquals("PONG", client.ping());
            assertEquals("HELLO_TEST", client.ping("HELLO_TEST"));

            // SET & GET
            assertTrue(client.set("name", "Prateekshaa"));
            assertEquals("Prateekshaa", client.get("name"));

            // String with spaces
            assertTrue(client.set("greeting", "Hello from Java TCP Client!"));
            assertEquals("Hello from Java TCP Client!", client.get("greeting"));

            // EXISTS
            assertTrue(client.exists("name"));
            assertFalse(client.exists("random_key"));

            // DBSIZE & KEYS
            assertEquals(2, client.dbsize());
            List<String> keys = client.keys();
            assertTrue(keys.contains("name"));
            assertTrue(keys.contains("greeting"));

            // TTL and PTTL
            assertTrue(client.ttl("greeting", 100));
            long pttl = client.pttl("greeting");
            assertTrue(pttl > 0 && pttl <= 100000);

            // SAVE
            String saveResp = client.save();
            assertTrue(saveResp.startsWith("OK"));

            // INFO
            String info = client.info();
            assertTrue(info.contains("version:1.0.0"));
            assertTrue(info.contains("current_keys:"));

            // DEL
            assertTrue(client.del("greeting"));
            assertNull(client.get("greeting"));
            assertEquals(1, client.dbsize());

            // FLUSHALL
            assertTrue(client.flushall());
            assertEquals(0, client.dbsize());
        }
    }

    @Test
    @DisplayName("Multiple simultaneous TCP clients operate concurrently")
    void testMultipleSimultaneousClients() throws Exception {
        int clientCount = 10;
        int opsPerClient = 25;
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(clientCount);
        AtomicInteger successCounter = new AtomicInteger(0);

        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            pool.submit(() -> {
                try {
                    startLatch.await();
                    try (KVClient client = new KVClient("127.0.0.1", port)) {
                        for (int i = 0; i < opsPerClient; i++) {
                            String k = "client-" + clientId + "-k-" + i;
                            String v = "val-" + i;
                            if (client.set(k, v)) {
                                String fetched = client.get(k);
                                if (v.equals(fetched)) {
                                    successCounter.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(completed, "All TCP clients should complete without timing out");
        assertEquals(clientCount * opsPerClient, successCounter.get(), 
                "Every client SET and GET operation over TCP must succeed");
    }
}
