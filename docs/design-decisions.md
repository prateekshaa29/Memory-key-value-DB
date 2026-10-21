# Design Decisions & Engineering Trade-Offs

This document captures the architectural decisions, trade-offs, and design rationale behind **ConcurrentKVStore**.

---

## 1. Concurrency Model: ReentrantReadWriteLock vs. Synchronized vs. StampedLock

### Considered Alternatives
- **Global `synchronized` block**: Simplest to implement, but serializes all operations. A high read volume completely blocks concurrent readers.
- **`StampedLock`**: Offers optimistic reading (`tryOptimisticRead()`). However, `StampedLock` is non-reentrant, prone to deadlocks if methods call internal helpers, and does not support condition variables easily.
- **`ReentrantReadWriteLock` (Selected)**:
  - Allows multiple simultaneous readers (`readLock()`) for non-mutating operations (`EXISTS`, `PTTL`, `KEYS`, `DBSIZE`, snapshot capture).
  - Serializes writes (`writeLock()`) for mutating operations (`SET`, `DEL`, `FLUSHALL`, eviction).
  - Configured with `fair = true` to prevent reader threads from starving writers during heavy read traffic.

### Why Separate `lruLock`?
In standard LRU caches, a `GET` must update recency, which is a mutation on the doubly linked list. If `GET` required the store's `writeLock()`, read concurrency would drop to 1.
**Our Solution**: `GET` executes with `readLock()` on the store dictionary, and only acquires a fine-grained `lruLock` for the few nanoseconds required to shuffle 4 pointers in the linked list.

---

## 2. LRU Eviction: Custom Doubly-Linked List vs. LinkedHashMap

### Why not `LinkedHashMap`?
Java provides `LinkedHashMap(initialCapacity, loadFactor, true)` which supports access-order eviction.
However:
1. `LinkedHashMap` is **not thread-safe**. Wrapping it with `Collections.synchronizedMap()` forces a coarse lock over the entire map for both reads and writes.
2. Iterating or removing entries during eviction requires internal collection locks, degrading throughput.
3. In placement interviews, using `LinkedHashMap` hides fundamental knowledge. Implementing a custom Doubly-Linked List demonstrates a solid understanding of data structures, pointer mechanics, and sentinel node patterns.

### Custom Doubly-Linked List + ConcurrentHashMap
- `Entry` doubles as a list node (containing `prev` and `next` pointers), avoiding secondary object allocations.
- Sentinel nodes (`headSentinel` for MRU and `tailSentinel` for LRU) eliminate null pointer edge cases during list insertions and deletions.
- All pointer manipulations are strictly $O(1)$.

---

## 3. Durability Guarantees: Snapshots vs. Write-Ahead Logging (WAL)

### Durability Model of ConcurrentKVStore
ConcurrentKVStore implements **point-in-time snapshot persistence** (equivalent to Redis RDB), **not** Write-Ahead Logging (WAL/AOF).

| Attribute | Snapshot Persistence (Implemented) | Write-Ahead Logging (WAL / AOF) |
|---|---|---|
| **Write Latency** | Minimal (writes occur in-memory at sub-microsecond speeds) | Higher (every write or batch must be appended to disk) |
| **Disk Overhead** | Compact binary state; no append bloat | Log file grows indefinitely without periodic compaction |
| **Recovery Speed** | Fast: loads final state directly | Slower: must replay all historical mutations |
| **RPO (Recovery Point Objective)** | **Up to the configured snapshot interval** (e.g., 60 seconds) | Sub-second (or zero with synchronous fsync) |

### Explicit Durability Disclaimer
> **Important Placement Concept**:
> Under snapshot persistence, any mutations that occur between the last successful snapshot and a crash will be lost. If the server crashes at $t = 59s$ with a 60-second snapshot interval, up to 59 seconds of recent writes may be lost.
> This matches the durability trade-off chosen by Redis RDB mode, trading strict zero data loss for raw in-memory throughput.

---

## 4. Timekeeping: Monotonic Clock vs. Wall-Clock Time

### The Fallacy of System.currentTimeMillis() for In-Process TTL
`System.currentTimeMillis()` represents wall-clock time. It is vulnerable to:
- Operating system NTP synchronization adjustments (which can shift time backward or forward).
- Leap seconds and daylight saving transitions.
- Manual clock modifications.

If the system clock steps backward by 5 seconds, keys might prematurely expire or survive longer than intended.

### Our Hybrid Timekeeping Solution
- **In-Memory Expiration**: Uses `System.nanoTime()`. Monotonic time guarantees that time only advances forward at a fixed frequency. Expiration checks use subtraction: `(System.nanoTime() - expiryNanoTime) >= 0`, which correctly handles numerical overflow.
- **Snapshot Persistence**: Stores absolute epoch milliseconds (`expiryEpochMs`). Because `nanoTime()` resets whenever the JVM or machine reboots, `nanoTime()` values are meaningless across restarts. When a snapshot is loaded on startup, `expiryEpochMs` is compared against current wall-clock time to restore remaining TTL.

---

## 5. Persistence Serialization: Custom Object-Independent Binary vs. Java Serialization

### Why Java Native Serialization (`Serializable` / `ObjectOutputStream`) was rejected:
1. **Security Vulnerabilities**: Java native deserialization is notoriously vulnerable to remote code execution (RCE) via gadget chains.
2. **JVM Lock-In**: Binary output from `ObjectOutputStream` is tightly coupled to JVM version, compiler details, and internal class signatures (`serialVersionUID`).
3. **Bloat**: Java serialization writes full class descriptors and metadata for every object, creating huge snapshot files.

### Custom Binary Format (`CKVS`)
- Uses `DataOutputStream` / `DataInputStream` with explicit UTF-8 string lengths.
- Magic header (`CKVS`) and format version ensure forward and backward compatibility.
- Stream-level CRC32 checksum guarantees bit-level data integrity and rejects corrupted or truncated snapshot files.

---

## 6. Network Layer: Multi-Threaded Sockets vs. Java NIO

### Why Bounded Thread Pool (`ServerSocket` + `ExecutorService`)?
1. **Clarity & Interview Friendliness**: A worker pool architecture clearly demonstrates thread lifecycle, bounded queues, and rejection policies.
2. **Low Latency for Moderate Concurrency**: Dedicated worker threads eliminate the epoll/kqueue selector loop overhead for hundreds of simultaneous connections.
3. **Resource Isolation**: A bounded `ThreadPoolExecutor` with `AbortPolicy` prevents denial-of-service (DoS) from connection exhaustion.
