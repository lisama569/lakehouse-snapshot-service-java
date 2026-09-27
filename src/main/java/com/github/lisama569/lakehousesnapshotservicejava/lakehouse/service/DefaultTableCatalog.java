package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LocalFileStore;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.TablePaths;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Filesystem-backed catalog; tables are opened lazily and cached per name. */
@Component
public class DefaultTableCatalog implements TableCatalog {

    private static final Logger log = LoggerFactory.getLogger(DefaultTableCatalog.class);

    private final Path root;
    private final ObjectMapper mapper;
    private final Map<String, LakehouseTable> tables = new ConcurrentHashMap<>();

    public DefaultTableCatalog(LakehouseProperties props, ObjectMapper mapper) {
        this.root = Path.of(props.getDataDir()).toAbsolutePath().normalize();
        this.mapper = mapper;
    }

    @Override
    public synchronized LakehouseTable createTable(String name, List<ColumnDef> columns,
                                                   String primaryKeyName) {
        LakehouseTable existing = tables.get(name);
        TablePaths paths = pathsOf(name);
        if (existing != null || new LocalFileStore(root, mapper).exists(paths.headFile())) {
            throw new LakehouseException(RejectReason.TABLE_EXISTS, "table already exists: " + name);
        }
        LakehouseTable table = LakehouseTable.create(store(), paths, name, columns, primaryKeyName);
        tables.put(name, table);
        log.info("catalog: created table name={} dir={} primaryKey={} initialSnapshot={}",
                name, paths.tableDir(), primaryKeyName, table.headSnapshotId());
        return table;
    }

    @Override
    public LakehouseTable open(String name) {
        return tables.computeIfAbsent(name, this::doOpen);
    }

    private LakehouseTable doOpen(String name) {
        LocalFileStore store = store();
        TablePaths paths = pathsOf(name);
        if (!store.exists(paths.headFile())) {
            throw new LakehouseException(RejectReason.UNKNOWN_TABLE, "unknown table: " + name);
        }
        LakehouseTable table = LakehouseTable.recover(store, paths, name);
        log.info("catalog: opened table name={} headSnapshot={} (recovered from {})",
                name, table.headSnapshotId(), paths.tableDir());
        return table;
    }

    @Override
    public boolean exists(String name) {
        return tables.containsKey(name) || store().exists(pathsOf(name).headFile());
    }

    private TablePaths pathsOf(String name) {
        if (name == null || !name.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new LakehouseException(RejectReason.UNKNOWN_TABLE,
                    "invalid table name: " + name);
        }
        return new TablePaths(root.resolve(name));
    }

    private LocalFileStore store() {
        return new LocalFileStore(root, mapper);
    }
}
