package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse.LakehouseTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Covers idempotent replay, offset gating, pk conflicts and type mismatch. */
class WriteSemanticsTest {

    @TempDir
    Path dir;

    @Test
    void duplicateBatchIsIdempotent() {
        LakehouseTable t = createStandard(dir, "orders");
        var first = t.submit(batch("orders", "b-1", 0, WriteMode.APPEND,
                row(1, 100L, 2, "alice", 3, 1.5, 4, true)), TIMEOUT);
        assertTrue(first.accepted(), "first write accepted");
        assertFalse(first.replay());
        assertEquals(1, first.newOffset());

        var replay = t.submit(batch("orders", "b-1", 0, WriteMode.APPEND,
                row(1, 100L, 2, "alice", 3, 1.5, 4, true)), TIMEOUT);
        verdict("duplicateBatchIsIdempotent", "first=" + first.snapshotId()
                + " replay=" + replay.snapshotId() + " replayFlag=" + replay.replay()
                + " offsetKept=" + replay.newOffset());
        assertTrue(replay.accepted());
        assertTrue(replay.replay(), "identical batchId is a replay, not a new commit");
        assertEquals(first.snapshotId(), replay.snapshotId());
        assertEquals(1, t.readHead().rows().size(), "no duplicate rows");
    }

    @Test
    void sameBatchIdWithDifferentPayloadIsBATCH_ID_REUSE() {
        LakehouseTable t = createStandard(dir, "orders");
        t.submit(batch("orders", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        BatchOutcome evil = t.submit(batch("orders", "b-1", 1, WriteMode.APPEND,
                row(1, 2L, 2, "different")), TIMEOUT);
        verdict("batchIdReuse", evil);
        assertFalse(evil.accepted());
        assertEquals(RejectReason.BATCH_ID_REUSE, evil.rejectReason());
        assertEquals(1, t.readHead().rows().size());
    }

    @Test
    void invalidOffsetIsRejected() {
        LakehouseTable t = createStandard(dir, "orders");
        t.submit(batch("orders", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        var gap = t.submit(batch("orders", "b-2", 5, WriteMode.APPEND, row(1, 2L, 2, "b")), TIMEOUT);
        verdict("invalidOffset", gap);
        assertFalse(gap.accepted());
        assertEquals(RejectReason.INVALID_OFFSET, gap.rejectReason());

        var correct = t.submit(batch("orders", "b-2", 1, WriteMode.APPEND, row(1, 2L, 2, "b")), TIMEOUT);
        assertTrue(correct.accepted(), "contiguous offset is accepted");
        assertEquals(2, correct.newOffset());
    }

    @Test
    void appendDuplicatePrimaryKeyIsRejected() {
        LakehouseTable t = createStandard(dir, "orders");
        t.submit(batch("orders", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        var dup = t.submit(batch("orders", "b-2", 1, WriteMode.APPEND, row(1, 1L, 2, "again")), TIMEOUT);
        verdict("appendPkConflict", dup);
        assertEquals(RejectReason.PK_CONFLICT, dup.rejectReason());
        assertEquals(1, t.readHead().rows().size(), "rejected batch wrote nothing");
        assertEquals(1, t.nextOffset(), "offset unchanged after rejection");
    }

    @Test
    void appendDuplicatePrimaryKeyInsideOneBatchIsRejected() {
        LakehouseTable t = createStandard(dir, "orders");
        var dup = t.submit(batch("orders", "b-1", 0, WriteMode.APPEND,
                row(1, 1L, 2, "a"), row(1, 1L, 2, "b")), TIMEOUT);
        verdict("appendPkConflictInBatch", dup);
        assertEquals(RejectReason.PK_CONFLICT, dup.rejectReason());
        assertEquals(0, t.readHead().rows().size(), "whole batch rejected atomically");
    }

    @Test
    void typeMismatchIsRejected() {
        LakehouseTable t = createStandard(dir, "orders");
        var bad = t.submit(batch("orders", "b-1", 0, WriteMode.APPEND,
                row(1, 1L, 2, 12345)), TIMEOUT); // name is STRING but value is number
        verdict("typeMismatch", bad);
        assertEquals(RejectReason.TYPE_MISMATCH, bad.rejectReason());
        assertEquals(0, t.readHead().rows().size());
    }

    @Test
    void missingNonNullablePrimaryKeyIsTypeMismatch() {
        LakehouseTable t = createStandard(dir, "orders");
        var bad = t.submit(batch("orders", "b-1", 0, WriteMode.APPEND, row(2, "nameless")), TIMEOUT);
        verdict("missingPk", bad);
        assertEquals(RejectReason.TYPE_MISMATCH, bad.rejectReason());
    }

    @Test
    void upsertUpdatesByPrimaryKey() {
        LakehouseTable t = createStandard(dir, "orders");
        t.submit(batch("orders", "b-1", 0, WriteMode.UPSERT,
                row(1, 7L, 2, "alice", 3, 1.0)), TIMEOUT);
        t.submit(batch("orders", "b-2", 1, WriteMode.UPSERT,
                row(1, 7L, 2, "alice2", 3, 9.9)), TIMEOUT);
        var rows = t.readHead().rows();
        verdict("upsert", "rows=" + rows);
        assertEquals(1, rows.size());
        assertEquals("alice2", rows.get(0).values().get("name"));
        assertEquals(9.9, (Double) rows.get(0).values().get("score"), 0.0001);
    }
}
