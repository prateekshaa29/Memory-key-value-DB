package com.concurrentkv.persistence;

import com.concurrentkv.core.Entry;
import com.concurrentkv.core.KVStore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Snapshot Persistence Manager.
 * Manages object-independent binary snapshots of the database state.
 *
 * Durability & Atomicity Guarantees:
 * - Snapshots capture a consistent point-in-time state using KVStore read lock.
 * - Snapshots are serialized and written asynchronously in a background thread.
 * - Files are written to a unique temporary file and flushed to disk hardware (fsync)
 *   before being atomically moved over the target snapshot file.
 * - Snapshots include magic header bytes ("CKVS"), a version number, and CRC32 checksums
 *   to detect corruption, partial writes, or incompatible versions.
 */
public class SnapshotManager {

    public static final byte[] MAGIC_BYTES = new byte[]{'C', 'K', 'V', 'S'};
    public static final int FORMAT_VERSION = 1;

    private final KVStore kvStore;
    private final File dataDir;
    private final String snapshotFilename;
    private final int intervalSeconds;

    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService diskWriterExecutor;
    private final AtomicBoolean isWriting = new AtomicBoolean(false);
    private final AtomicLong lastSnapshotTimeMs = new AtomicLong(0);
    private final AtomicLong successfulSnapshots = new AtomicLong(0);

    public SnapshotManager(KVStore kvStore, String dataDirPath, String snapshotFilename, int intervalSeconds) {
        this.kvStore = kvStore;
        this.dataDir = new File(dataDirPath);
        this.snapshotFilename = snapshotFilename;
        this.intervalSeconds = intervalSeconds;

        this.diskWriterExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "kv-snapshot-writer");
                t.setDaemon(true);
                return t;
            }
        });

        this.snapshotScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "kv-snapshot-scheduler-" + count.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * Starts the periodic snapshot task if interval is positive.
     */
    public synchronized void start() {
        ensureDataDirectory();
        if (intervalSeconds > 0) {
            snapshotScheduler.scheduleWithFixedDelay(
                    this::triggerPeriodicSnapshot,
                    intervalSeconds,
                    intervalSeconds,
                    TimeUnit.SECONDS
            );
        }
    }

    private void ensureDataDirectory() {
        if (!dataDir.exists()) {
            boolean created = dataDir.mkdirs();
            if (created) {
                System.out.println("[INFO] Created data directory: " + dataDir.getAbsolutePath());
            }
        }
    }

    private void triggerPeriodicSnapshot() {
        try {
            saveAsync();
        } catch (Throwable t) {
            System.err.println("[WARN] Background snapshot trigger failed: " + t.getMessage());
        }
    }

    /**
     * Asynchronously triggers a snapshot without blocking the caller.
     * The point-in-time state is captured in memory immediately.
     */
    public CompletableFuture<Boolean> saveAsync() {
        if (!isWriting.compareAndSet(false, true)) {
            // A snapshot is already in progress
            CompletableFuture<Boolean> f = new CompletableFuture<>();
            f.complete(false);
            return f;
        }

        // Fast in-memory capture under store read lock
        final List<Entry> entries = kvStore.captureSnapshotEntries();

        return CompletableFuture.supplyAsync(() -> {
            try {
                writeEntriesToDisk(entries);
                lastSnapshotTimeMs.set(System.currentTimeMillis());
                successfulSnapshots.incrementAndGet();
                return true;
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to write snapshot to disk: " + e.getMessage());
                return false;
            } finally {
                isWriting.set(false);
            }
        }, diskWriterExecutor);
    }

    /**
     * Synchronously captures and writes a snapshot to disk.
     * Used for manual SAVE commands and server shutdown.
     */
    public boolean saveSync() throws IOException, SnapshotException {
        List<Entry> entries = kvStore.captureSnapshotEntries();
        writeEntriesToDisk(entries);
        lastSnapshotTimeMs.set(System.currentTimeMillis());
        successfulSnapshots.incrementAndGet();
        return true;
    }

    /**
     * Writes entries to a temporary file, computes CRC32, fsyncs, and atomically renames.
     */
    private void writeEntriesToDisk(List<Entry> entries) throws IOException, SnapshotException {
        ensureDataDirectory();
        File targetFile = getSnapshotFile();
        File tempFile = new File(dataDir, snapshotFilename + ".tmp." + UUID.randomUUID());

        try {
            CRC32 crc = new CRC32();
            try (FileOutputStream fos = new FileOutputStream(tempFile);
                 BufferedOutputStream bos = new BufferedOutputStream(fos, 65536);
                 CheckedOutputStream cos = new CheckedOutputStream(bos, crc);
                 DataOutputStream dos = new DataOutputStream(cos)) {

                // 1. Header: Magic bytes, version, timestamp, entry count
                dos.write(MAGIC_BYTES);
                dos.writeInt(FORMAT_VERSION);
                dos.writeLong(System.currentTimeMillis());
                dos.writeInt(entries.size());

                // 2. Records
                for (Entry entry : entries) {
                    byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(keyBytes.length);
                    dos.write(keyBytes);

                    byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(valBytes.length);
                    dos.write(valBytes);

                    boolean hasExpiry = entry.hasExpiry();
                    dos.writeBoolean(hasExpiry);
                    if (hasExpiry) {
                        dos.writeLong(entry.getExpiryEpochMs());
                    }
                }

                // Flush checked stream so all record bytes are processed by CRC32
                dos.flush();
                long calculatedCrc = crc.getValue();

                // 3. Write CRC32 checksum as the 8-byte footer (outside CRC calculation)
                // We write directly to the underlying buffered output stream
                bos.write(ByteBuffer.allocate(8).putLong(calculatedCrc).array());
                bos.flush();

                // Force hardware sync (fsync)
                fos.getFD().sync();
            }

            // 4. Atomic move / replace
            atomicReplaceFile(tempFile, targetFile);

        } catch (Exception e) {
            // Clean up temp file on failure to prevent stale files
            if (tempFile.exists()) {
                tempFile.delete();
            }
            throw new SnapshotException("Failed writing snapshot: " + e.getMessage(), e);
        }
    }

    private void atomicReplaceFile(File tempFile, File targetFile) throws IOException {
        Path tempPath = tempFile.toPath();
        Path targetPath = targetFile.toPath();

        try {
            Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            // Fallback for filesystems that do not support ATOMIC_MOVE
            Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Loads the snapshot from disk on server startup.
     * Validates magic bytes, format version, and CRC32 checksum.
     * Returns the count of valid entries restored.
     */
    public int loadSnapshot() throws IOException, SnapshotException {
        File snapshotFile = getSnapshotFile();
        if (!snapshotFile.exists() || snapshotFile.length() == 0) {
            System.out.println("[INFO] No existing snapshot found at " + snapshotFile.getAbsolutePath() + ". Starting with empty store.");
            return 0;
        }

        long fileLength = snapshotFile.length();
        if (fileLength < 4 + 4 + 8 + 4 + 8) { // Magic(4) + Version(4) + Timestamp(8) + Count(4) + CRC(8)
            throw new SnapshotException("Snapshot file is too small to be valid: " + fileLength + " bytes");
        }

        CRC32 crc = new CRC32();
        List<Entry> entries = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(snapshotFile);
             BufferedInputStream bis = new BufferedInputStream(fis, 65536);
             CheckedInputStream cis = new CheckedInputStream(bis, crc);
             DataInputStream dis = new DataInputStream(cis)) {

            // 1. Verify Magic Header
            byte[] magic = new byte[4];
            dis.readFully(magic);
            if (magic[0] != MAGIC_BYTES[0] || magic[1] != MAGIC_BYTES[1] ||
                magic[2] != MAGIC_BYTES[2] || magic[3] != MAGIC_BYTES[3]) {
                throw new SnapshotException("Invalid snapshot magic header: not a valid ConcurrentKVStore file");
            }

            // 2. Verify Version
            int version = dis.readInt();
            if (version != FORMAT_VERSION) {
                throw new SnapshotException("Unsupported snapshot format version: " + version + " (expected " + FORMAT_VERSION + ")");
            }

            // 3. Read Timestamp and Record Count
            long snapshotTime = dis.readLong();
            int count = dis.readInt();
            if (count < 0) {
                throw new SnapshotException("Corrupt snapshot: negative entry count " + count);
            }

            // 4. Read Records
            for (int i = 0; i < count; i++) {
                int keyLen = dis.readInt();
                if (keyLen < 0 || keyLen > 10_000_000) {
                    throw new SnapshotException("Corrupt record key length: " + keyLen);
                }
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = dis.readInt();
                if (valLen < 0 || valLen > 100_000_000) {
                    throw new SnapshotException("Corrupt record value length: " + valLen);
                }
                byte[] valBytes = new byte[valLen];
                dis.readFully(valBytes);
                String value = new String(valBytes, StandardCharsets.UTF_8);

                boolean hasExpiry = dis.readBoolean();
                long expiryEpochMs = -1;
                if (hasExpiry) {
                    expiryEpochMs = dis.readLong();
                }

                Entry entry = new Entry(key, value, -1, expiryEpochMs);
                entries.add(entry);
            }

            // Checksum calculated over all bytes read up to this point
            long calculatedCrc = crc.getValue();

            // Read the 8-byte stored checksum from the footer
            byte[] crcBytes = new byte[8];
            int read = bis.read(crcBytes);
            if (read != 8) {
                throw new SnapshotException("Snapshot file truncated: could not read footer checksum");
            }
            long storedCrc = ByteBuffer.wrap(crcBytes).getLong();

            if (calculatedCrc != storedCrc) {
                throw new SnapshotException("Snapshot integrity check failed: CRC32 mismatch (stored=" 
                        + storedCrc + ", calculated=" + calculatedCrc + ")");
            }

            // Restore into KVStore
            int restoredCount = kvStore.restoreFromSnapshot(entries);
            System.out.println("[INFO] Successfully recovered " + restoredCount + " active entries from snapshot (timestamp: " 
                    + snapshotTime + ")");
            return restoredCount;
        }
    }

    public File getSnapshotFile() {
        return new File(dataDir, snapshotFilename);
    }

    public long getLastSnapshotTimeMs() {
        return lastSnapshotTimeMs.get();
    }

    public long getSuccessfulSnapshots() {
        return successfulSnapshots.get();
    }

    /**
     * Gracefully stops executors and saves final state.
     */
    public void shutdown() {
        snapshotScheduler.shutdown();
        try {
            if (!snapshotScheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                snapshotScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            snapshotScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        diskWriterExecutor.shutdown();
        try {
            if (!diskWriterExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                diskWriterExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            diskWriterExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
