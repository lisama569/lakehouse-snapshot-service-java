package com.github.lisama569.lakehousesnapshotservicejava.storage;

import com.github.lisama569.lakehousesnapshotservicejava.model.BatchReceipt;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.SnapshotInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 表的持久化状态（JSON 序列化到 state/state.json）。
 * 字段公开以便引擎层直接操作；所有变更只发生在持锁的临界区内。
 *
 * <p>注意：列与快照在 JSON 中以 List 存储（而不是以数字为键的 Map），
 * 避免 Jackson 反序列化 Map 时把数字键统一转成 String 的类型陷阱。</p>
 */
public class TableState {

    /** 列默认值：列 id -> 默认值。 */
    public static class ColumnDefault {
        public int columnId;
        public Object value;

        public ColumnDefault() {
        }

        public ColumnDefault(int columnId, Object value) {
            this.columnId = columnId;
            this.value = value;
        }
    }

    public String table;
    public long createdAt;
    public int schemaVersion;
    /** 列按 id 排序的列表，顺序即列顺序。 */
    public List<ColumnDef> columns = new ArrayList<>();
    /** 主键列名列表，建表后不可变。 */
    public List<String> primaryKey = new ArrayList<>();
    /** 已提交位点：已成功提交的批次数。 */
    public long committedOffset;
    /** 下一个列 id。 */
    public int nextColumnId = 1;
    /** 新增列的默认值（旧数据在新模式下读取时填充）。 */
    public List<ColumnDefault> columnDefaults = new ArrayList<>();
    /** 下一个快照 id（从 1 开始）。 */
    public long nextSnapshotId = 1;
    /** 当前链头快照 id，空表为 0。 */
    public long headSnapshotId;
    /** 保留中的快照列表（按 id 有序）。 */
    public List<SnapshotInfo> snapshots = new ArrayList<>();
    /** batchId -> 回执（去重 / 重放依据）。 */
    public Map<String, BatchReceipt> batches = new LinkedHashMap<>();

    public TableState() {
    }

    public ColumnDef columnById(int id) {
        for (ColumnDef c : columns) {
            if (c.id() == id) {
                return c;
            }
        }
        return null;
    }

    public ColumnDef columnByName(String name) {
        for (ColumnDef c : columns) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    public SnapshotInfo snapshotById(long id) {
        for (SnapshotInfo s : snapshots) {
            if (s.snapshotId() == id) {
                return s;
            }
        }
        return null;
    }

    public Object columnDefault(int columnId) {
        for (ColumnDefault d : columnDefaults) {
            if (d.columnId == columnId) {
                return d.value;
            }
        }
        return null;
    }

    public void putColumnDefault(int columnId, Object value) {
        columnDefaults.removeIf(d -> d.columnId == columnId);
        columnDefaults.add(new ColumnDefault(columnId, value));
    }

    public void removeColumnDefault(int columnId) {
        columnDefaults.removeIf(d -> d.columnId == columnId);
    }
}
