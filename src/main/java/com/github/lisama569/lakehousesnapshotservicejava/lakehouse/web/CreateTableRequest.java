package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.web;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import java.util.List;

public record CreateTableRequest(List<ColumnDef> columns, String primaryKey) {
}
