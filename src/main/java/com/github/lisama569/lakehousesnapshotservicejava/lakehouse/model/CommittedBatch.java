package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/** Idempotency ledger entry for a batch already applied. */
public record CommittedBatch(String batchId, long expectedOffset, long resultingOffset,
                             String snapshotId, WriteMode mode) {
}
