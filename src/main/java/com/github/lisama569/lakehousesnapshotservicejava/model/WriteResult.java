package com.github.lisama569.lakehousesnapshotservicejava.model;

/** 写入结果，重放成功时 replay=true 且返回的是首次提交的快照标识。 */
public record WriteResult(
        String table,
        String batchId,
        BatchStatus status,
        RejectReason rejectReason,
        long snapshotId,
        int schemaVersion,
        long committedOffset,
        int writtenRows,
        boolean replay,
        String detail
) {
}
