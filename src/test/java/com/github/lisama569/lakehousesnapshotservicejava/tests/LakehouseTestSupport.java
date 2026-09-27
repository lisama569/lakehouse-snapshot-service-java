package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.config.LakehouseProperties;
import com.github.lisama569.lakehousesnapshotservicejava.engine.FaultControl;
import com.github.lisama569.lakehousesnapshotservicejava.engine.LocalTable;
import com.github.lisama569.lakehousesnapshotservicejava.engine.TableCatalog;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteRequest;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableStateStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** 构造纯本地、临时目录的引擎，测试不依赖 Spring 与任何外部服务。 */
final class LakehouseTestSupport {

    final ObjectMapper mapper = new ObjectMapper();
    final TableCatalog catalog;
    final FaultControl faultControl;
    final TableStateStore store;

    LakehouseTestSupport(Path dataDir, long batchTimeoutMs) {
        LakehouseProperties props = new LakehouseProperties();
        props.setDataDir(dataDir.toString());
        props.setDefaultBatchTimeoutMs(batchTimeoutMs);
        props.setMaintenanceTimeoutMs(batchTimeoutMs);
        props.setDefaultRetainSnapshots(2);
        LockManager lockManager = new LockManager();
        this.store = new TableStateStore(dataDir, mapper);
        this.faultControl = new FaultControl();
        this.catalog = new TableCatalog(props, store, lockManager, mapper, faultControl);
    }

    static List<ColumnDef> orderColumns() {
        return List.of(
                new ColumnDef(1, "id", DataType.LONG),
                new ColumnDef(2, "name", DataType.STRING),
                new ColumnDef(3, "amount", DataType.DOUBLE));
    }

    void createOrders(String table) {
        catalog.create(table, orderColumns(), List.of("id"));
    }

    LocalTable orders(String table) {
        return catalog.get(table);
    }

    static RowRecord row(long id, String name, double amount) {
        return new RowRecord(Map.of("id", id, "name", name, "amount", amount));
    }

    static WriteRequest append(String batchId, long expectedOffset, List<RowRecord> rows) {
        return new WriteRequest("orders", batchId, WriteMode.APPEND, expectedOffset, 0, rows);
    }

    static WriteRequest upsert(String batchId, long expectedOffset, List<RowRecord> rows) {
        return new WriteRequest("orders", batchId, WriteMode.UPSERT, expectedOffset, 0, rows);
    }

    static WriteRequest append(String table, String batchId, long expectedOffset, List<RowRecord> rows) {
        return new WriteRequest(table, batchId, WriteMode.APPEND, expectedOffset, 0, rows);
    }

    static WriteRequest upsert(String table, String batchId, long expectedOffset, List<RowRecord> rows) {
        return new WriteRequest(table, batchId, WriteMode.UPSERT, expectedOffset, 0, rows);
    }
}
