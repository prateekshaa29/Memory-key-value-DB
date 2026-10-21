package com.concurrentkv.persistence;

import com.concurrentkv.core.KVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Snapshot Persistence and Recovery Tests")
public class PersistenceTest {

    private File tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("kv-persist-test").toFile();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && tempDir.exists()) {
            Files.walk(tempDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Snapshot saves and restores keys accurately")
    void testSaveAndRestore() throws Exception {
        KVStore originalStore = new KVStore(50);
        originalStore.set("user:101", "Prateekshaa");
        originalStore.set("user:102", "Alex");
        originalStore.set("config:theme", "dark");

        SnapshotManager smOriginal = new SnapshotManager(originalStore, tempDir.getAbsolutePath(), "test_snapshot.ckv", 0);
        assertTrue(smOriginal.saveSync());

        File snapshotFile = smOriginal.getSnapshotFile();
        assertTrue(snapshotFile.exists(), "Snapshot file should be created");
        assertTrue(snapshotFile.length() > 0, "Snapshot file should not be empty");

        // Restore into a completely new store
        KVStore newStore = new KVStore(50);
        SnapshotManager smRestore = new SnapshotManager(newStore, tempDir.getAbsolutePath(), "test_snapshot.ckv", 0);
        int restoredCount = smRestore.loadSnapshot();

        assertEquals(3, restoredCount);
        assertEquals(3, newStore.dbsize());
        assertEquals("Prateekshaa", newStore.get("user:101"));
        assertEquals("Alex", newStore.get("user:102"));
        assertEquals("dark", newStore.get("config:theme"));
    }

    @Test
    @DisplayName("Expired keys are skipped during snapshot restore")
    void testRestoreFiltersExpiredKeys() throws Exception {
        KVStore originalStore = new KVStore(50);
        originalStore.set("activeKey", "activeVal");
        originalStore.set("shortLived", "tempVal", 1); // 1 sec TTL

        SnapshotManager smOriginal = new SnapshotManager(originalStore, tempDir.getAbsolutePath(), "expire_snapshot.ckv", 0);
        assertTrue(smOriginal.saveSync());

        // Wait for shortLived to expire
        Thread.sleep(1200);

        KVStore newStore = new KVStore(50);
        SnapshotManager smRestore = new SnapshotManager(newStore, tempDir.getAbsolutePath(), "expire_snapshot.ckv", 0);
        int restoredCount = smRestore.loadSnapshot();

        assertEquals(1, restoredCount, "Only active key should be restored");
        assertEquals("activeVal", newStore.get("activeKey"));
        assertNull(newStore.get("shortLived"), "Expired key should not be in restored store");
    }

    @Test
    @DisplayName("Corrupt magic header is detected and rejected")
    void testCorruptedMagicHeader() throws Exception {
        File snapshotFile = new File(tempDir, "corrupt.ckv");
        try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
            // Write wrong magic header
            fos.write(new byte[]{'B', 'A', 'D', '!'});
            fos.write(new byte[32]); // some padding
        }

        KVStore store = new KVStore(10);
        SnapshotManager sm = new SnapshotManager(store, tempDir.getAbsolutePath(), "corrupt.ckv", 0);

        assertThrows(SnapshotException.class, sm::loadSnapshot);
    }

    @Test
    @DisplayName("CRC32 mismatch detects corrupted or tampered file")
    void testCrc32CorruptionDetection() throws Exception {
        KVStore store = new KVStore(50);
        store.set("secret", "important_data");

        SnapshotManager sm = new SnapshotManager(store, tempDir.getAbsolutePath(), "tampered.ckv", 0);
        sm.saveSync();

        File file = sm.getSnapshotFile();
        assertTrue(file.exists());

        // Tamper with one byte in the middle of the payload
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(20); // within the record bytes
            byte original = raf.readByte();
            raf.seek(20);
            raf.writeByte(original ^ 0xFF); // flip bits
        }

        KVStore restoreStore = new KVStore(50);
        SnapshotManager smTampered = new SnapshotManager(restoreStore, tempDir.getAbsolutePath(), "tampered.ckv", 0);

        // Checksum mismatch must trigger SnapshotException
        assertThrows(SnapshotException.class, smTampered::loadSnapshot);
    }
}
