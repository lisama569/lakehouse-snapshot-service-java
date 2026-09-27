package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.List;

/** Inbound batch: batchId makes replays idempotent; offset is the ingestion position. */
public record BatchRequest(String table, String batchId, long expectedOffset, WriteMode mode,
                           List<RowRecord> rows) {
}
