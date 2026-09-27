package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/** 某一版本的表模式。 */
public record SchemaSpec(int version, List<ColumnDef> columns, List<String> primaryKey) {
}
