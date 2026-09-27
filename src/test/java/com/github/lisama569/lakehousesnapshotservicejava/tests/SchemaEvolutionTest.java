package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.ReadResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 模式演进的读兼容、默认值、不错列与不兼容拒绝验证。 */
class SchemaEvolutionTest {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionTest.class);

    @TempDir
    Path tempDir;

    private LakehouseTestSupport support;

    @BeforeEach
    void setUp() {
        support = new LakehouseTestSupport(tempDir, 5_000);
        support.createOrders("orders");
    }

    private void expectReason(RejectReason reason, Runnable action) {
        LakehouseException ex = assertThrows(LakehouseException.class, action::run);
        assertEquals(reason, ex.getReason());
        log.info("[test] rejected reason={} detail={}", ex.getReason(), ex.getMessage());
    }

    @Test
    void addRenameDropColumnKeepsOldDataReadableWithoutColumnShift() {
        var table = support.orders("orders");
        // v1 旧数据
        var s1 = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "alice", 10.0))));
        long oldSnapshotId = s1.snapshotId();

        // 新增列 tag（默认 null）
        table.evolve(new SchemaChange(SchemaChange.ChangeType.ADD, "tag", null,
                DataType.STRING, null));
        // 重命名 amount -> price（列 id 不变）
        table.evolve(new SchemaChange(SchemaChange.ChangeType.RENAME, "amount", "price",
                null, null));

        // 读旧快照：时间旅行必须以“旧快照被读取时的当前模式”解释，旧行补齐新列、改名列按 id 命中。
        ReadResult oldRead = table.read(oldSnapshotId);
        Map<String, Object> expected = new java.util.HashMap<>();
        expected.put("id", 1L);
        expected.put("name", "alice");
        expected.put("price", 10.0);
        expected.put("tag", null);
        assertEquals(expected, oldRead.rows().get(0).values());
        assertTrue(oldRead.defaultFilledColumns().contains("tag"),
                "旧数据缺少新增列，应标记默认值填充");
        log.info("[test] time-travel snapshotId={} schemaVersion={} row={} defaultFilled={}",
                oldRead.snapshotId(), oldRead.schemaVersion(),
                oldRead.rows().get(0).values(), oldRead.defaultFilledColumns());

        // 删除 name 列
        table.evolve(new SchemaChange(SchemaChange.ChangeType.DROP, "name", null, null, null));

        ReadResult current = table.read(null);
        Map<String, Object> expectedAfterDrop = new java.util.HashMap<>();
        expectedAfterDrop.put("id", 1L);
        expectedAfterDrop.put("price", 10.0);
        expectedAfterDrop.put("tag", null);
        assertEquals(expectedAfterDrop, current.rows().get(0).values());
        log.info("[test] after-drop row={} columns implied={}",
                current.rows().get(0).values(), current.rows().get(0).values().keySet());
    }

    @Test
    void newSchemaWritesAreInvisibleToOldSnapshot() {
        var table = support.orders("orders");
        WriteResult first = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        table.write(LakehouseTestSupport.append("orders", "b2", 1L,
                List.of(LakehouseTestSupport.row(2, "b", 2.0))));

        ReadResult oldView = table.read(first.snapshotId());
        assertEquals(1, oldView.rows().size(), "旧快照不能看到之后写入的行");
        assertEquals(1L, oldView.rows().get(0).values().get("id"));
        assertEquals(2, table.read(null).rows().size());
        log.info("[test] snapshot-isolation oldSnapshotId={} oldRows={} currentRows={}",
                first.snapshotId(), 1, 2);
    }

    @Test
    void writesAfterAddUseNewColumnAndRenamedColumnIsNotShifted() {
        var table = support.orders("orders");
        table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        table.evolve(new SchemaChange(SchemaChange.ChangeType.ADD, "flag", null,
                DataType.BOOLEAN, null));
        table.evolve(new SchemaChange(SchemaChange.ChangeType.RENAME, "name", "customer",
                null, null));

        var r = table.write(LakehouseTestSupport.upsert("orders", "u1", 1L,
                List.of(new RowRecord(Map.of("id", 2L, "customer", "bob", "amount", 5.0,
                        "flag", true)))));
        ReadResult read = table.read(r.snapshotId());
        // 旧行按列 id 对齐：customer(name 的 id=2) 不会错列到 amount(id=3)
        var row1 = read.rows().stream().filter(x -> x.values().get("id").equals(1L)).findFirst().orElseThrow();
        assertEquals("a", row1.values().get("customer"));
        assertEquals(1.0, row1.values().get("amount"));
        assertNull(row1.values().get("flag"));
        var row2 = read.rows().stream().filter(x -> x.values().get("id").equals(2L)).findFirst().orElseThrow();
        assertEquals("bob", row2.values().get("customer"));
        assertEquals(true, row2.values().get("flag"));
        log.info("[test] no-column-shift oldRow={} newRow={}", row1.values(), row2.values());
    }

    @Test
    void incompatibleChangesAreRejected() {
        var table = support.orders("orders");
        // 新增重名列
        expectReason(RejectReason.SCHEMA_INCOMPATIBLE, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.ADD, "id", null, DataType.STRING, null)));
        // 重命名到已存在列名
        expectReason(RejectReason.SCHEMA_INCOMPATIBLE, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.RENAME, "name", "id", null, null)));
        // 对不存在的列改名 / 删除
        expectReason(RejectReason.SCHEMA_INCOMPATIBLE, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.RENAME, "ghost", "g2", null, null)));
        expectReason(RejectReason.SCHEMA_INCOMPATIBLE, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.DROP, "ghost", null, null, null)));
        // 主键列禁止删除
        expectReason(RejectReason.SCHEMA_INCOMPATIBLE, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.DROP, "id", null, null, null)));
        // 默认值类型不符
        expectReason(RejectReason.TYPE_MISMATCH, () -> table.evolve(
                new SchemaChange(SchemaChange.ChangeType.ADD, "x", null, DataType.LONG, "abc")));
        // 被拒绝后模式版本不变
        assertEquals(1, table.schema().version());
    }

    @Test
    void addColumnDefaultValueAppliesToOldRows() {
        var table = support.orders("orders");
        var s1 = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        table.evolve(new SchemaChange(SchemaChange.ChangeType.ADD, "tier", null,
                DataType.LONG, 7L));
        ReadResult oldView = table.read(s1.snapshotId());
        assertEquals(7L, oldView.rows().get(0).values().get("tier"),
                "旧行在新模式下必须得到稳定、可解释的默认值");
        assertTrue(oldView.defaultFilledColumns().contains("tier"));
        log.info("[test] default-value snapshotId={} tier={}", s1.snapshotId(),
                oldView.rows().get(0).values().get("tier"));
    }
}
