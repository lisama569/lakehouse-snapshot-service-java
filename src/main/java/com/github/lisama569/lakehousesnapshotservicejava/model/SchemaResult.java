package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/** 当前模式查询结果。 */
public record SchemaResult(
        String table,
        int version,
        List<ColumnDef> columns,
        List<String> primaryKey,
        long snapshotId,
        long updatedAt
) {
}
