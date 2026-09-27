package com.github.lisama569.lakehousesnapshotservicejava.model;

/**
 * 拒绝 / 失败原因，调用方依据此枚举区分：位点非法、主键冲突、类型不匹配、
 * 批次超时、模式不兼容等，错误码语义稳定。
 */
public enum RejectReason {
    BAD_REQUEST,
    TABLE_NOT_FOUND,
    TABLE_ALREADY_EXISTS,
    INVALID_OFFSET,
    PK_CONFLICT,
    MISSING_PRIMARY_KEY,
    UNKNOWN_COLUMN,
    TYPE_MISMATCH,
    DUPLICATE_BATCH_CONFLICT,
    SCHEMA_INCOMPATIBLE,
    BATCH_TIMEOUT,
    MAINTENANCE_TIMEOUT,
    SNAPSHOT_NOT_FOUND
}
