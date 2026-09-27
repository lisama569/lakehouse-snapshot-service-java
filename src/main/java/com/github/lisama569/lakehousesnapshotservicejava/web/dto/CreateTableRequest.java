package com.github.lisama569.lakehousesnapshotservicejava.web.dto;

import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;

import java.util.List;

/** 建表请求。 */
public record CreateTableRequest(String table, List<ColumnDef> columns, List<String> primaryKey) {
}
