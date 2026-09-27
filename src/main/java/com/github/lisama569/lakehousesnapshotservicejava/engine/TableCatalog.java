package com.github.lisama569.lakehousesnapshotservicejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.config.LakehouseProperties;
import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaSpec;
import com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableState;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 表目录：建表、按名获取已打开的表、列举本地已有表。 */
public class TableCatalog {

    private static final Logger log = LoggerFactory.getLogger(TableCatalog.class);

    private final LakehouseProperties properties;
    private final TableStateStore store;
    private final LockManager lockManager;
    private final ObjectMapper mapper;
    private final FaultControl faultControl;
    private final Map<String, LocalTable> openTables = new ConcurrentHashMap<>();

    public TableCatalog(LakehouseProperties properties,
                        TableStateStore store,
                        LockManager lockManager,
                        ObjectMapper mapper,
                        FaultControl faultControl) {
        this.properties = properties;
        this.store = store;
        this.lockManager = lockManager;
        this.mapper = mapper;
        this.faultControl = faultControl;
    }

    /** 获取表，不存在抛 TABLE_NOT_FOUND。 */
    public LocalTable get(String table) {
        if (!store.exists(table)) {
            throw new LakehouseException(RejectReason.TABLE_NOT_FOUND, "表不存在: " + table);
        }
        return openTables.computeIfAbsent(table, this::newTable);
    }

    /** 存在则打开，不存在返回 null。 */
    public LocalTable getOrOpen(String table) {
        if (!store.exists(table)) {
            return null;
        }
        return openTables.computeIfAbsent(table, this::newTable);
    }

    private LocalTable newTable(String table) {
        return new LocalTable(table, store, lockManager, mapper, faultControl,
                properties.getDefaultBatchTimeoutMs());
    }

    /**
     * 建表。列必须非空、列名唯一；主键列必须全部存在于列定义中。
     * 已存在同名表抛 TABLE_ALREADY_EXISTS。
     */
    public SchemaResult create(String table, List<ColumnDef> columns, List<String> primaryKey) {
        if (columns == null || columns.isEmpty()) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "建表必须提供至少一列");
        }
        List<String> pk = primaryKey == null ? List.of() : primaryKey;
        Set<String> names = new LinkedHashSet<>();
        List<ColumnDef> normalized = new ArrayList<>();
        int id = 1;
        for (ColumnDef c : columns) {
            if (c == null || c.name() == null || c.name().isBlank() || c.type() == null) {
                throw new LakehouseException(RejectReason.BAD_REQUEST,
                        "列定义非法（需要 name 与 type）: " + c);
            }
            if (!names.add(c.name())) {
                throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                        "列名重复: " + c.name());
            }
            normalized.add(new ColumnDef(id, c.name(), c.type()));
            id++;
        }
        for (String key : pk) {
            if (!names.contains(key)) {
                throw new LakehouseException(RejectReason.BAD_REQUEST,
                        "主键列 '" + key + "' 不在列定义中: " + names);
            }
        }
        if (store.exists(table)) {
            throw new LakehouseException(RejectReason.TABLE_ALREADY_EXISTS,
                    "表已存在: " + table);
        }

        TableState state = new TableState();
        state.table = table;
        state.createdAt = System.currentTimeMillis();
        state.schemaVersion = 1;
        state.nextColumnId = normalized.size() + 1;
        state.primaryKey = new ArrayList<>(pk);
        for (ColumnDef c : normalized) {
            state.columns.add(c);
        }
        state.committedOffset = 0;
        state.nextSnapshotId = 1;
        state.headSnapshotId = 0L;
        store.persist(state);

        log.info("[lakehouse] table-created table={} schemaVersion=1 columns={} pk={}",
                table, names, pk);
        return new SchemaResult(table, 1, normalized, List.copyOf(pk), 0L, state.createdAt);
    }

    public List<String> list() {
        return store.listTables();
    }

    /** 从持久化状态构造当前模式视图。 */
    public static SchemaSpec toSchemaSpec(TableState state) {
        return new SchemaSpec(state.schemaVersion, List.copyOf(state.columns),
                List.copyOf(state.primaryKey));
    }
}
