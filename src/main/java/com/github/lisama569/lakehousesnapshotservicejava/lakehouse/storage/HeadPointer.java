package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

/**
 * The single mutable pointer. It is published last (after the immutable commit
 * record), making commit publication effectively atomic: a commit whose record
 * exists but is not reachable from this pointer is treated as aborted on recovery.
 */
public record HeadPointer(String snapshotId, long seq) {
}
