package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.List;

public record ReadOutcome(String snapshotId, int schemaVersion, List<RowView> rows) {
}
