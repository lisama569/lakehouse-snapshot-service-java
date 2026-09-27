package com.github.lisama569.lakehousesnapshotservicejava.web;

import com.github.lisama569.lakehousesnapshotservicejava.engine.FaultControl;
import com.github.lisama569.lakehousesnapshotservicejava.engine.TableCatalog;
import com.github.lisama569.lakehousesnapshotservicejava.model.ReadResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteRequest;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import com.github.lisama569.lakehousesnapshotservicejava.service.MaintenanceService;
import com.github.lisama569.lakehousesnapshotservicejava.web.dto.CreateTableRequest;
import com.github.lisama569.lakehousesnapshotservicejava.web.dto.EvolveRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** 表管理、写入、时间旅行读取、模式演进、维护操作的 REST 入口。 */
@RestController
@RequestMapping("/api/tables")
public class TableController {

    private final TableCatalog catalog;
    private final MaintenanceService maintenance;
    private final FaultControl faultControl;

    public TableController(TableCatalog catalog, MaintenanceService maintenance,
                           FaultControl faultControl) {
        this.catalog = catalog;
        this.maintenance = maintenance;
        this.faultControl = faultControl;
    }

    @PostMapping
    public SchemaResult create(@RequestBody CreateTableRequest request) {
        return catalog.create(request.table(), request.columns(), request.primaryKey());
    }

    @GetMapping
    public Map<String, Object> list() {
        List<String> tables = catalog.list();
        return Map.of("tables", tables, "count", tables.size());
    }

    @GetMapping("/{table}")
    public SchemaResult schema(@PathVariable String table) {
        return catalog.get(table).schema();
    }

    @PostMapping("/{table}/writes")
    public WriteResult write(@PathVariable String table, @RequestBody WriteRequest request) {
        WriteRequest effective = new WriteRequest(table, request.batchId(), request.mode(),
                request.expectedOffset(), request.timeoutMs(), request.rows());
        return catalog.get(table).write(effective);
    }

    @GetMapping("/{table}/rows")
    public ReadResult read(@PathVariable String table,
                           @RequestParam(value = "snapshot", required = false) Long snapshotId) {
        return catalog.get(table).read(snapshotId);
    }

    @PostMapping("/{table}/schema")
    public SchemaResult evolve(@PathVariable String table, @RequestBody EvolveRequest request) {
        if (request.changes() == null || request.changes().isEmpty()) {
            return catalog.get(table).schema();
        }
        var localTable = catalog.get(table);
        for (var change : request.changes()) {
            localTable.evolve(change);
        }
        return localTable.schema();
    }

    @PostMapping("/{table}/compact")
    public WriteResult compact(@PathVariable String table,
                               @RequestParam(value = "timeoutMs", required = false) Long timeoutMs) {
        return maintenance.compact(table, timeoutMs);
    }

    @PostMapping("/{table}/expire")
    public Object expire(@PathVariable String table,
                         @RequestParam(value = "retain", required = false) Integer retain,
                         @RequestParam(value = "timeoutMs", required = false) Long timeoutMs) {
        return maintenance.expire(table, retain, timeoutMs);
    }

    @PostMapping("/{table}/fault")
    public Map<String, String> armFault(@PathVariable String table) {
        catalog.get(table); // 校验表存在
        faultControl.armCrashBeforeCommit();
        return Map.of("status", "ARMED", "table", table);
    }

    @DeleteMapping("/{table}/fault")
    public Map<String, String> clearFault(@PathVariable String table) {
        faultControl.clear();
        return Map.of("status", "CLEARED", "table", table);
    }
}
