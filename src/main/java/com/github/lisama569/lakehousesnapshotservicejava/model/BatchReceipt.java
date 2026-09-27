package com.github.lisama569.lakehousesnapshotservicejava.model;

/**
 * 批次回执，持久化在表状态中，用于幂等去重：
 * 同一 batchId 重复提交直接返回首次结果。
 */
public record BatchReceipt(
        String batchId,
        BatchStatus status,
        RejectReason rejectReason,
        String payloadHash,
        long snapshotId,
        long offset,
        long updatedAt,
        String detail
) {
}
