package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.Map;

/**
 * A row keyed by column id (not by name/position). This keeps reads stable
 * across rename/drop/add operations.
 */
public record RowRecord(Map<Integer, Object> valuesByColumnId) {
}
