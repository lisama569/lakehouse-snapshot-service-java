package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.web;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType;

/**
 * Wire form of a schema change. Exactly one of add/rename/drop is intended;
 * {@code op} is ADD / RENAME / DROP.
 */
public record SchemaChangeRequest(String op, String name, String to,
                                  ColumnType type, Boolean nullable) {
}
