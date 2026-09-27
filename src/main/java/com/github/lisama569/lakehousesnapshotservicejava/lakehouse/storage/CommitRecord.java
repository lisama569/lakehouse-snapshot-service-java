package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.CommittedBatch;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SnapshotManifest;

/**
 * Durable, immutable commit journal entry. The journal is the source of truth
 * used during crash recovery. {@code batch} is non-null only for write commits.
 */
public record CommitRecord(long seq, SnapshotManifest manifest, CommittedBatch batch) {

    public static CommitRecord of(SnapshotManifest manifest) {
        return new CommitRecord(manifest.seq(), manifest, null);
    }

    public static CommitRecord of(SnapshotManifest manifest, CommittedBatch batch) {
        return new CommitRecord(manifest.seq(), manifest, batch);
    }
}
