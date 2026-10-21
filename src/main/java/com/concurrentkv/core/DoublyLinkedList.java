package com.concurrentkv.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Custom Doubly Linked List for O(1) LRU (Least Recently Used) cache tracking.
 * Uses dummy head (Most Recently Used) and dummy tail (Least Recently Used) sentinels
 * to eliminate boundary condition checks.
 *
 * NOTE: This data structure is not self-synchronized; calls must be guarded by
 * the database locking strategy (e.g., KVStore lock).
 */
public class DoublyLinkedList {

    private final Entry headSentinel;
    private final Entry tailSentinel;
    private int size;

    public DoublyLinkedList() {
        headSentinel = new Entry("__LRU_HEAD_SENTINEL__", "");
        tailSentinel = new Entry("__LRU_TAIL_SENTINEL__", "");
        headSentinel.next = tailSentinel;
        tailSentinel.prev = headSentinel;
        size = 0;
    }

    /**
     * Adds an entry directly after the head sentinel (marks as Most Recently Used).
     */
    public void addToHead(Entry node) {
        if (node == null) return;
        node.next = headSentinel.next;
        node.prev = headSentinel;
        headSentinel.next.prev = node;
        headSentinel.next = node;
        size++;
    }

    /**
     * Moves an existing entry in the list to the head (refreshes recency on GET/UPDATE).
     */
    public void moveToHead(Entry node) {
        if (node == null || node.prev == null || node.next == null) return;
        // Detach from current position
        node.prev.next = node.next;
        node.next.prev = node.prev;

        // Attach after head sentinel
        node.next = headSentinel.next;
        node.prev = headSentinel;
        headSentinel.next.prev = node;
        headSentinel.next = node;
    }

    /**
     * Removes an entry from anywhere in the doubly linked list.
     */
    public void remove(Entry node) {
        if (node == null || node.prev == null || node.next == null) return;
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
        size--;
    }

    /**
     * Removes and returns the entry immediately before the tail sentinel (the LRU entry).
     * Returns null if the list is empty.
     */
    public Entry removeTail() {
        if (isEmpty()) {
            return null;
        }
        Entry lruNode = tailSentinel.prev;
        remove(lruNode);
        return lruNode;
    }

    /**
     * Peeks at the least recently used entry without removing it.
     */
    public Entry peekTail() {
        if (isEmpty()) {
            return null;
        }
        return tailSentinel.prev;
    }

    public boolean isEmpty() {
        return headSentinel.next == tailSentinel;
    }

    public int size() {
        return size;
    }

    public void clear() {
        headSentinel.next = tailSentinel;
        tailSentinel.prev = headSentinel;
        size = 0;
    }

    /**
     * Returns a snapshot of entries from Most Recently Used to Least Recently Used.
     */
    public List<Entry> getAllMruToLru() {
        List<Entry> list = new ArrayList<>(size);
        Entry current = headSentinel.next;
        while (current != tailSentinel && current != null) {
            list.add(current);
            current = current.next;
        }
        return list;
    }

    /**
     * Returns a snapshot of entries from Least Recently Used to Most Recently Used.
     * Useful for restoring or inspecting LRU order.
     */
    public List<Entry> getAllLruToMru() {
        List<Entry> list = new ArrayList<>(size);
        Entry current = tailSentinel.prev;
        while (current != headSentinel && current != null) {
            list.add(current);
            current = current.prev;
        }
        return list;
    }
}
