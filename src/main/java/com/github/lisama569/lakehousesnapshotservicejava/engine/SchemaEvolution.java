package com.github.lisama569.lakehousesnapshotservicejava.engine;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.ColumnDef;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.storage.TableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 模式演进规则（在表提交锁内应用，整组变更原子生效）：
 * <ul>
 *   <li>ADD COLUMN：允许。新列获得新的稳定列 id；defaultValue 可空（默认 null），
 *       旧行读取缺失该列时按默认值填充，行为可解释。</li>
 *   <li>RENAME COLUMN：允许。仅改列名，列 id 不变，旧数据文件按 id 解码，绝不错列；
 *       若被改名的是主键列，主键名同步更新。</li>
 *   <li>DROP COLUMN：允许（从当前模式移除）。旧文件中的残留字段读取时忽略；
 *       主键列禁止删除。</li>
 * </ul>
 * 以下情况拒绝（SCHEMA_INCOMPATIBLE / BAD_REQUEST），状态不变：
 * 列名非法 / 重名、对不存在列变更、类型或默认值与声明不符等。
 * 本实现不提供“改类型”，任何隐含的类型修改都会被拒绝。
 */
public final class SchemaEvolution {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolution.class);

    private SchemaEvolution() {
    }

    public static void apply(TableState state, SchemaChange change) {
        if (change == null || change.op() == null) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "模式变更缺少 op");
        }
        switch (change.op()) {
            case ADD -> applyAdd(state, change);
            case RENAME -> applyRename(state, change);
            case DROP -> applyDrop(state, change);
        }
        state.schemaVersion++;
        log.info("[lakehouse] schema-change applied table={} op={} column={} newVersion={}",
                state.table, change.op(), change.column(), state.schemaVersion);
    }

    private static void applyAdd(TableState state, SchemaChange change) {
        String name = change.column();
        if (name == null || name.isBlank()) {
            throw new LakehouseException(RejectReason.BAD_REQUEST, "ADD COLUMN 缺少列名");
        }
        if (change.dataType() == null) {
            throw new LakehouseException(RejectReason.BAD_REQUEST,
                    "ADD COLUMN '" + name + "' 缺少 dataType");
        }
        if (findByName(state, name) != null) {
            throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "列 '" + name + "' 已存在，重复添加会造成模式歧义");
        }
        Object dflt = change.defaultValue();
        validateDefault(name, change.dataType(), dflt);

        int id = state.nextColumnId++;
        state.columns.add(new ColumnDef(id, name, change.dataType()));
        state.putColumnDefault(id, dflt);
    }

    private static void applyRename(TableState state, SchemaChange change) {
        String oldName = change.column();
        String newName = change.renameTo();
        ColumnDef existing = requireColumn(state, oldName);
        if (newName == null || newName.isBlank()) {
            throw new LakehouseException(RejectReason.BAD_REQUEST,
                    "RENAME COLUMN '" + oldName + "' 缺少新列名 renameTo");
        }
        if (findByName(state, newName) != null) {
            throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "目标列名 '" + newName + "' 已存在，重命名会造成列冲突");
        }
        // 保持列 id 不变：旧数据文件按 id 解码，这是不错列的关键。
        int idx = indexOfColumn(state, existing.id());
        state.columns.set(idx, new ColumnDef(existing.id(), newName, existing.type()));
        for (int i = 0; i < state.primaryKey.size(); i++) {
            if (state.primaryKey.get(i).equals(oldName)) {
                state.primaryKey.set(i, newName);
            }
        }
    }

    private static void applyDrop(TableState state, SchemaChange change) {
        String name = change.column();
        ColumnDef existing = requireColumn(state, name);
        if (state.primaryKey.contains(name)) {
            throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "主键列 '" + name + "' 不允许删除");
        }
        state.columns.removeIf(c -> c.id() == existing.id());
        state.removeColumnDefault(existing.id());
    }

    private static int indexOfColumn(TableState state, int id) {
        for (int i = 0; i < state.columns.size(); i++) {
            if (state.columns.get(i).id() == id) {
                return i;
            }
        }
        return -1;
    }

    private static ColumnDef requireColumn(TableState state, String name) {
        ColumnDef c = findByName(state, name);
        if (c == null) {
            throw new LakehouseException(RejectReason.SCHEMA_INCOMPATIBLE,
                    "列 '" + name + "' 不存在，当前列=" + names(state));
        }
        return c;
    }

    private static ColumnDef findByName(TableState state, String name) {
        for (ColumnDef c : state.columns) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    private static java.util.List<String> names(TableState state) {
        return state.columns.stream().map(ColumnDef::name).toList();
    }

    private static void validateDefault(String name, DataType type, Object value) {
        if (value == null) {
            return;
        }
        boolean ok = switch (type) {
            case STRING -> value instanceof String;
            case BOOLEAN -> value instanceof Boolean;
            case LONG -> value instanceof Integer || value instanceof Long;
            case DOUBLE -> value instanceof Number;
        };
        if (!ok) {
            throw new LakehouseException(RejectReason.TYPE_MISMATCH,
                    "新列 '" + name + "' 默认值类型与声明类型 " + type + " 不符: " + value);
        }
    }

    static Map<String, ColumnDef> columnsByName(TableState state) {
        return RowCodec.byName(state);
    }
}
