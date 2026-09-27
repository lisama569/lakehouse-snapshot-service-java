package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RowRecord;
import java.util.List;

/** On-disk payload of an immutable data file. */
public record DataFilePayload(int schemaVersion, long seq, List<RowRecord> rows) {
}
