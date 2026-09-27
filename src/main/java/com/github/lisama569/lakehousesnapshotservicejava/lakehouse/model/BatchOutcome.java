package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/**
 * Result of submitting a batch. {@code replay=true} means an identical batchId
 * was already committed and this call changed nothing (idempotent replay).
 */
public record BatchOutcome(boolean accepted, boolean replay, String snapshotId, long newOffset,
                           RejectReason rejectReason, String detail) {

    public static BatchOutcome committed(String snapshotId, long newOffset) {
        return new BatchOutcome(true, false, snapshotId, newOffset, null, null);
    }

    public static BatchOutcome replayed(String snapshotId, long newOffset) {
        return new BatchOutcome(true, true, snapshotId, newOffset, null, null);
    }

    public static BatchOutcome rejected(RejectReason reason, String detail) {
        return new BatchOutcome(false, false, null, -1L, reason, detail);
    }
}
