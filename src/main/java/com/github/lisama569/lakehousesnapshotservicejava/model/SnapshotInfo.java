package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/** 不可变快照的元数据。 */
public record SnapshotInfo(
        long snapshotId,
        long parentId,
        int schemaVersion,
        long createdAt,
        long recordCount,
        List<String> dataFiles,
        String operation,
        String batchId
) {
}
