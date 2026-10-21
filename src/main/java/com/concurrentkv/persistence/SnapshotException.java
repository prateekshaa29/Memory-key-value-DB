package com.concurrentkv.persistence;

public class SnapshotException extends Exception {
    public SnapshotException(String message) {
        super(message);
    }

    public SnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}
