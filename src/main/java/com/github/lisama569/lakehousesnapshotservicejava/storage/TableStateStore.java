package com.github.lisama569.lakehousesnapshotservicejava.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 表状态的加载与原子持久化。
 *
 * <p>目录布局（全部本地）：</p>
 * <pre>
 * {dataDir}/{table}/state/state.json   —— 元数据（原子覆盖写）
 * {dataDir}/{table}/state/commit.lock  —— 跨进程文件锁
 * {dataDir}/{table}/data/snap-{id}-{uuid}.jsonl —— 不可变数据文件
 * {dataDir}/{table}/staging/           —— 临时文件，崩溃后可回收
 * </pre>
 */
public class TableStateStore {

    private static final Logger log = LoggerFactory.getLogger(TableStateStore.class);
    private static final Pattern VALID_TABLE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_\\-]{0,63}");

    private final Path baseDir;
    private final ObjectMapper mapper;

    public TableStateStore(Path baseDir, ObjectMapper mapper) {
        this.baseDir = baseDir.toAbsolutePath();
        this.mapper = mapper;
    }

    public Path baseDir() {
        return baseDir;
    }

    public Path tableDir(String table) {
        validateTableName(table);
        return baseDir.resolve(table);
    }

    public Path stateFile(String table) {
        return tableDir(table).resolve("state").resolve("state.json");
    }

    public Path lockFile(String table) {
        return tableDir(table).resolve("state").resolve("commit.lock");
    }

    public Path dataDir(String table) {
        return tableDir(table).resolve("data");
    }

    public Path stagingDir(String table) {
        return tableDir(table).resolve("staging");
    }

    private void validateTableName(String table) {
        if (table == null || !VALID_TABLE.matcher(table).matches()) {
            throw new LakehouseException(RejectReason.BAD_REQUEST,
                    "非法表名（允许字母数字、下划线、短横线，1-64 字符）: " + table);
        }
    }

    public boolean exists(String table) {
        return Files.isRegularFile(stateFile(table));
    }

    /** 加载状态。注意：不在此处回收 staging，因为无锁的读路径也会调用本方法，
     *  回收其它提交者正在发布的临时文件会破坏并发提交。staging 回收只在持锁临界区内做。 */
    public TableState load(String table) {
        Path sf = stateFile(table);
        if (!Files.isRegularFile(sf)) {
            return null;
        }
        try {
            TableState state = mapper.readValue(sf.toFile(), TableState.class);
            if (state.columns == null) {
                state.columns = new java.util.ArrayList<>();
            }
            if (state.snapshots == null) {
                state.snapshots = new java.util.ArrayList<>();
            }
            if (state.columnDefaults == null) {
                state.columnDefaults = new java.util.ArrayList<>();
            }
            if (state.batches == null) {
                state.batches = new java.util.LinkedHashMap<>();
            }
            if (state.primaryKey == null) {
                state.primaryKey = List.of();
            }
            return state;
        } catch (IOException e) {
            throw new UncheckedIOException("加载表状态失败: " + sf, e);
        }
    }

    /** 原子持久化：临时文件 + ATOMIC_MOVE，任何时刻 state.json 都是完整 JSON。 */
    public void persist(TableState state) {
        try {
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state);
            AtomicFiles.writeAtomically(stateFile(state.table), json);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化表状态失败 table=" + state.table, e);
        }
    }

    /** 列举本地已有表（含 state.json 的目录）。 */
    public List<String> listTables() {
        if (!Files.isDirectory(baseDir)) {
            return List.of();
        }
        try (Stream<Path> dirs = Files.list(baseDir)) {
            return dirs.filter(Files::isDirectory)
                    .filter(d -> Files.isRegularFile(d.resolve("state").resolve("state.json")))
                    .map(d -> d.getFileName().toString())
                    .sorted(Comparator.naturalOrder())
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("列举表目录失败: " + baseDir, e);
        }
    }
}
