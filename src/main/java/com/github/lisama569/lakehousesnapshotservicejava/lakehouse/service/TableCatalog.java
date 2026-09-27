package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import java.util.List;

/** Creates/opens tables from the local data directory. */
public interface TableCatalog {

    LakehouseTable createTable(String name, List<ColumnDef> columns, String primaryKeyName);

    LakehouseTable open(String name);

    boolean exists(String name);
}
