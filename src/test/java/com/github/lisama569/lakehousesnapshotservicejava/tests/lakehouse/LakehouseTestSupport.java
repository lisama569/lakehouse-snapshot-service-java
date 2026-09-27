package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchRequest;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LocalFileStore;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.TablePaths;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.io.TempDir;

/** Shared fixture: local tables under a JUnit temp directory with rich assertion logging. */
public final class LakehouseTestSupport {

    public static final Duration TIMEOUT = Duration.ofSeconds(5);

    private LakehouseTestSupport() {
    }

    public static ObjectMapper mapper() {
        return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static LocalFileStore store(Path dir) {
        return new LocalFileStore(dir, mapper());
    }

    /** Standard schema: id LONG PK, name STRING, score DOUBLE, active BOOLEAN. */
    public static List<ColumnDef> standardColumns() {
        return List.of(
                new ColumnDef(1, "id", ColumnType.LONG, false),
                new ColumnDef(2, "name", ColumnType.STRING, true),
                new ColumnDef(3, "score", ColumnType.DOUBLE, true),
                new ColumnDef(4, "active", ColumnType.BOOLEAN, true));
    }

    public static LakehouseTable createStandard(Path dir, String table) {
        return LakehouseTable.create(store(dir), new TablePaths(dir.resolve(table)), table,
                standardColumns(), "id");
    }

    public static LakehouseTable recover(Path dir, String table) {
        return LakehouseTable.recover(store(dir), new TablePaths(dir.resolve(table)), table);
    }

    public static RowRecord row(Object... kv) {
        Map<Integer, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((Integer) kv[i], kv[i + 1]);
        }
        return new RowRecord(m);
    }

    public static BatchRequest batch(String table, String batchId, long expectedOffset,
                                     WriteMode mode, RowRecord... rows) {
        return new BatchRequest(table, batchId, expectedOffset, mode, new ArrayList<>(List.of(rows)));
    }

    public static void verdict(String test, Object result) {
        System.out.println("[VERDICT:" + test + "] " + result);
    }
}
