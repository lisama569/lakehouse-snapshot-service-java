package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/** Distinct rejection reasons for batch writes, schema evolution and maintenance. */
public enum RejectReason {
    /** Ingestion offset is not contiguous with the committed offset. */
    INVALID_OFFSET,
    /** Primary key already exists (APPEND) or is missing/duplicated illegally. */
    PK_CONFLICT,
    /** Value type is incompatible with the column, or a required value is missing. */
    TYPE_MISMATCH,
    /** Timed out while waiting for the serial commit boundary. */
    BATCH_TIMEOUT,
    /** Schema change conflicts with existing schema or existing data. */
    SCHEMA_INCOMPATIBLE,
    /** Same batchId replayed with a different payload. */
    BATCH_ID_REUSE,
    /** Referenced table does not exist. */
    UNKNOWN_TABLE,
    /** Referenced snapshot has expired or never existed. */
    UNKNOWN_SNAPSHOT,
    /** Maintenance operation could not acquire its concurrency boundary in time. */
    MAINTENANCE_BUSY,
    /** Local IO failure during staging/publish; nothing was published. */
    IO_FAILURE,
    /** Table already exists on creation. */
    TABLE_EXISTS
}
