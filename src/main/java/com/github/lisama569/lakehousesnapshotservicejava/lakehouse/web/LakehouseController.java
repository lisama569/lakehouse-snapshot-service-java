package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.web;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchRequest;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.MaintenanceOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ReadOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChangeOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service.MaintenanceService;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service.SchemaService;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service.SnapshotReadService;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service.TableCatalog;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service.WriteService;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tables")
public class LakehouseController {

    private final TableCatalog catalog;
    private final WriteService writes;
    private final SchemaService schemas;
    private final SnapshotReadService reads;
    private final MaintenanceService maintenance;

    public LakehouseController(TableCatalog catalog, WriteService writes, SchemaService schemas,
                               SnapshotReadService reads, MaintenanceService maintenance) {
        this.catalog = catalog;
        this.writes = writes;
        this.schemas = schemas;
        this.reads = reads;
        this.maintenance = maintenance;
    }

    @PostMapping("/{name}")
    public ResponseEntity<Map<String, Object>> createTable(@PathVariable String name,
                                                           @RequestBody(required = false) CreateTableRequest req) {
        CreateTableRequest body = req != null ? req
                : new CreateTableRequest(List.of(new ColumnDef(0, "id",
                com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType.LONG, false)),
                "id");
        String pk = body.primaryKey() != null ? body.primaryKey() : "id";
        var table = catalog.createTable(name, body.columns(), pk);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("table", name, "initialSnapshot", table.headSnapshotId()));
    }

    @PostMapping("/{name}/batches")
    public BatchOutcome write(@PathVariable String name, @RequestBody BatchRequest request) {
        BatchRequest effective = new BatchRequest(name, request.batchId(), request.expectedOffset(),
                request.mode(), request.rows());
        BatchOutcome outcome = writes.submit(effective, null);
        return outcome;
    }

    @PostMapping("/{name}/schema")
    public SchemaChangeOutcome evolve(@PathVariable String name, @RequestBody SchemaChangeRequest req) {
        SchemaChange change = toChange(req);
        return schemas.evolve(name, change, null);
    }

    @GetMapping("/{name}/rows")
    public ReadOutcome readHead(@PathVariable String name) {
        return reads.readHead(name);
    }

    @GetMapping("/{name}/snapshots/{snapshotId}/rows")
    public ReadOutcome readAt(@PathVariable String name, @PathVariable String snapshotId) {
        return reads.readAt(name, snapshotId);
    }

    @PostMapping("/{name}/compact")
    public MaintenanceOutcome compact(@PathVariable String name) {
        return maintenance.compact(name, null);
    }

    @PostMapping("/{name}/expire")
    public MaintenanceOutcome expire(@PathVariable String name,
                                     @RequestParam(defaultValue = "2") int retain) {
        return maintenance.expireSnapshots(name, retain, null);
    }

    private SchemaChange toChange(SchemaChangeRequest req) {
        if (req == null || req.op() == null) {
            throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "schema change requires op=ADD|RENAME|DROP");
        }
        return switch (req.op().toUpperCase()) {
            case "ADD" -> new SchemaChange.AddColumn(req.name(), req.type(),
                    req.nullable() == null || req.nullable());
            case "RENAME" -> new SchemaChange.RenameColumn(req.name(), req.to());
            case "DROP" -> new SchemaChange.DropColumn(req.name());
            default -> throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "unknown schema op: " + req.op());
        };
    }
}
