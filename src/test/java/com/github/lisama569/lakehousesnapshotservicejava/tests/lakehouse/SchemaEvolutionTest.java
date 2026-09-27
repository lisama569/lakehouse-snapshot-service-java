package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse.LakehouseTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ReadOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SchemaEvolutionTest {

    @TempDir
    Path dir;

    @Test
    void addColumnGivesStableExplainedDefaultToOldRows() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "alice")), TIMEOUT);
        var add = t.evolve(new SchemaChange.AddColumn("remark",
                com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType.STRING, true), TIMEOUT);
        verdict("addColumn", add);
        assertTrue(add.accepted());

        ReadOutcome head = t.readHead();
        var oldRow = head.rows().get(0);
        System.out.println("[VERDICT:addColumn] oldRow under new schema=" + oldRow.values()
                + " sources=" + oldRow.sources());
        assertNull(oldRow.values().get("remark"), "added column defaults to null on old rows");
        assertEquals("DEFAULT_NULL", oldRow.sources().get("remark"), "default is explicitly explained");
        assertEquals("FROM_DATA", oldRow.sources().get("name"));
        assertEquals(2, head.schemaVersion());
    }

    @Test
    void addingNonNullableColumnToNonEmptyTableIsRejected() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L)), TIMEOUT);
        var rejected = t.evolve(new SchemaChange.AddColumn("must",
                com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType.STRING, false), TIMEOUT);
        verdict("addNonNullableRejected", rejected);
        assertFalse(rejected.accepted());
        assertEquals(RejectReason.SCHEMA_INCOMPATIBLE, rejected.rejectReason());
        assertEquals(1, t.schema().version(), "schema unchanged");
    }

    @Test
    void renameDoesNotMisalignDataAndReusesColumnId() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "alice")), TIMEOUT);
        String beforeSnapshot = t.headSnapshotId();
        var rename = t.evolve(new SchemaChange.RenameColumn("name", "full_name"), TIMEOUT);
        verdict("rename", rename);
        assertTrue(rename.accepted());

        ReadOutcome head = t.readHead();
        assertEquals("alice", head.rows().get(0).values().get("full_name"),
                "rename preserves the value (same stable column id)");
        assertFalse(head.rows().get(0).values().containsKey("name"));

        // old snapshot still uses old names and cannot see anything written after it
        ReadOutcome old = t.readAt(beforeSnapshot);
        assertEquals("alice", old.rows().get(0).values().get("name"));
        assertNull(old.rows().get(0).values().get("full_name"));
        System.out.println("[VERDICT:rename] newView=" + head.rows().get(0).values()
                + " oldView=" + old.rows().get(0).values());
    }

    @Test
    void renameToExistingNameIsRejected() {
        LakehouseTable t = createStandard(dir, "t");
        var rejected = t.evolve(new SchemaChange.RenameColumn("name", "score"), TIMEOUT);
        verdict("renameConflict", rejected);
        assertEquals(RejectReason.SCHEMA_INCOMPATIBLE, rejected.rejectReason());
    }

    @Test
    void dropColumnHidesDataInNewSchemaButOldSnapshotKeepsIt() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "alice", 3, 4.0)), TIMEOUT);
        String s1 = t.headSnapshotId();
        var drop = t.evolve(new SchemaChange.DropColumn("score"), TIMEOUT);
        verdict("drop", drop);
        assertTrue(drop.accepted());

        ReadOutcome head = t.readHead();
        assertFalse(head.rows().get(0).values().containsKey("score"), "dropped column absent in new view");
        ReadOutcome old = t.readAt(s1);
        assertEquals(4.0, old.rows().get(0).values().get("score"), "time travel keeps dropped data");
        System.out.println("[VERDICT:drop] newView=" + head.rows().get(0).values()
                + " oldView=" + old.rows().get(0).values());
    }

    @Test
    void droppingPrimaryKeyIsRejected() {
        LakehouseTable t = createStandard(dir, "t");
        var rejected = t.evolve(new SchemaChange.DropColumn("id"), TIMEOUT);
        verdict("dropPk", rejected);
        assertEquals(RejectReason.SCHEMA_INCOMPATIBLE, rejected.rejectReason());
    }

    @Test
    void writesAfterSchemaChangeAreInvisibleToOldSnapshot() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "first")), TIMEOUT);
        String s1 = t.headSnapshotId();
        t.evolve(new SchemaChange.AddColumn("remark",
                com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ColumnType.STRING, true), TIMEOUT);
        t.submit(batch("t", "b-2", 1, WriteMode.APPEND, row(1, 2L, 2, "second", 5, "hello")), TIMEOUT);

        ReadOutcome oldView = t.readAt(s1);
        ReadOutcome headView = t.readHead();
        System.out.println("[VERDICT:visibility] oldRows=" + oldView.rows().size()
                + " headRows=" + headView.rows().size());
        assertEquals(1, oldView.rows().size(), "old snapshot cannot see row written after it");
        assertEquals(2, headView.rows().size(), "head sees both");
        assertEquals(1, oldView.schemaVersion());
    }
}
