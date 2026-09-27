package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.List;

/**
 * Immutable snapshot: a schema version plus the exact set of immutable data
 * files visible at a point in time.
 *
 * @param expiredSnapshotSeqs snapshot seqs expired by an EXPIRE commit (cumulative knowledge for recovery)
 * @param signatureSeen hash of the originating batch payload (0 for non-write snapshots)
 */
public record SnapshotManifest(String snapshotId, long parentSnapshotSeq, long seq,
                               long createdAtMillis, int schemaVersion, long nextOffset,
                               List<String> dataFiles, String kind, String message,
                               List<Long> expiredSnapshotSeqs, int signatureSeen) {

    public SnapshotManifest {
        dataFiles = List.copyOf(dataFiles);
        expiredSnapshotSeqs = List.copyOf(expiredSnapshotSeqs);
    }

    /** Convenience for non-write snapshots. */
    public SnapshotManifest(String snapshotId, long parentSnapshotSeq, long seq,
                            long createdAtMillis, int schemaVersion, long nextOffset,
                            List<String> dataFiles, String kind, String message,
                            List<Long> expiredSnapshotSeqs) {
        this(snapshotId, parentSnapshotSeq, seq, createdAtMillis, schemaVersion, nextOffset,
                dataFiles, kind, message, expiredSnapshotSeqs, 0);
    }
}
