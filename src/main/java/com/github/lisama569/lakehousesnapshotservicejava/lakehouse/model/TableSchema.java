package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.List;

/**
 * An immutable, versioned schema. Columns are addressed by stable id; the
 * primary key references a column id.
 */
public record TableSchema(int version, int nextColumnId, int primaryKeyColumnId, List<ColumnDef> columns) {
}
