package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/**
 * Immutable column definition. Columns carry a stable {@code id} so that rename
 * and drop are not confused with positional shifts (silent column misalignment).
 */
public record ColumnDef(int id, String name, ColumnType type, boolean nullable) {
}
