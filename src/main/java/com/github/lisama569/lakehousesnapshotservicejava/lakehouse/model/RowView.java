package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.Map;

/**
 * A row projected for the reader. {@code sources} explains each field: FROM_DATA,
 * DEFAULT_NULL (column added after the row was written) or DROPPED.
 */
public record RowView(Map<String, Object> values, Map<String, String> sources) {
}
