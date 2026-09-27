package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse.LakehouseTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SnapshotMaintenanceTest {

    @TempDir
    Path dir;

    private LakehouseTable twoBatches() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND,
                row(1, 1L, 2, "a"), row(1, 2L, 2, "b")), TIMEOUT);
        t.submit(batch("t", "b-2", 1, WriteMode.APPEND,
                row(1, 3L, 2, "c")), TIMEOUT);
        return t;
    }

    @Test
    void timeTravelReturnsHistoricalContent() {
        LakehouseTable t = twoBatches();
        String s1 = t.snapshots().get(1).snapshotId(); // snapshot after first batch
        var at1 = t.readAt(s1);
        var head = t.readHead();
        System.out.println("[VERDICT:timeTravel] s1=" + s1 + " rowsAtS1=" + at1.rows().size()
                + " rowsAtHead=" + head.rows().size());
        assertEquals(2, at1.rows().size());
        assertEquals(3, head.rows().size());
        assertEquals(s1, at1.snapshotId());
    }

    @Test
    void unknownSnapshotIsDistinctRejection() {
        LakehouseTable t = twoBatches();
        LakehouseException ex = assertThrows(LakehouseException.class, () -> t.readAt("s999"));
        verdict("unknownSnapshot", ex.reason());
        assertEquals(RejectReason.UNKNOWN_SNAPSHOT, ex.reason());
    }

    @Test
    void compactionMergesFilesWithoutChangingRowsAndKeepsOldSnapshots() {
        LakehouseTable t = twoBatches();
        int filesBefore = t.snapshots().get(t.snapshots().size() - 1).dataFiles().size();
        String s1 = t.snapshots().get(1).snapshotId();
        var outcome = t.compact(TIMEOUT);
        verdict("compact", "done=" + outcome.done() + " snapshot=" + outcome.snapshotId()
                + " files " + outcome.filesBefore() + "->" + outcome.filesAfter());
        assertTrue(outcome.done());
        assertEquals(filesBefore, outcome.filesBefore());
        assertEquals(1, outcome.filesAfter());
        assertEquals(3, t.readHead().rows().size(), "rows preserved");
        assertEquals(2, t.readAt(s1).rows().size(), "old snapshot still fully readable after compaction");
    }

    @Test
    void upsertThenCompactKeepsOnlyLatestVersion() {
        LakehouseTable t = createStandard(dir, "u");
        t.submit(batch("u", "b-1", 0, WriteMode.UPSERT, row(1, 1L, 2, "v1")), TIMEOUT);
        t.submit(batch("u", "b-2", 1, WriteMode.UPSERT, row(1, 1L, 2, "v2")), TIMEOUT);
        var outcome = t.compact(TIMEOUT);
        verdict("upsertCompact", outcome);
        var rows = t.readHead().rows();
        assertEquals(1, rows.size());
        assertEquals("v2", rows.get(0).values().get("name"));
    }

    @Test
    void expireRemovesOldSnapshotsAndTheirPrivateFilesButKeepsHeadData() {
        LakehouseTable t = twoBatches();
        long beforeSnapshots = t.snapshots().size();
        String goneInit = t.snapshots().get(0).snapshotId();
        String goneData = t.snapshots().get(1).snapshotId();
        // After compaction the head points only at the merged file, making the old data
        // files private to the expired snapshots and eligible for physical reclamation.
        t.compact(TIMEOUT);
        var outcome = t.expireSnapshots(1, TIMEOUT);
        verdict("expire", "done=" + outcome.done() + " removedFiles=" + outcome.removedFiles()
                + " newSnapshot=" + outcome.snapshotId());
        assertTrue(outcome.done());
        assertTrue(outcome.removedFiles().size() >= 1, "physical files unique to expired snapshot are deleted");
        assertEquals(3, t.readHead().rows().size(), "head data intact");
        for (String gone : java.util.List.of(goneInit, goneData)) {
            LakehouseException ex = assertThrows(LakehouseException.class, () -> t.readAt(gone));
            assertEquals(RejectReason.UNKNOWN_SNAPSHOT, ex.reason(),
                    "expired snapshot " + gone + " cannot be read");
        }
        assertTrue(t.snapshots().size() < beforeSnapshots);
    }

    @Test
    void expireTwiceIsStableAndRepeatedExpiryKeepsHead() {
        LakehouseTable t = twoBatches();
        String secondDataSnapshot = t.snapshots().get(2).snapshotId();
        t.compact(TIMEOUT); // more snapshots and a new head
        var first = t.expireSnapshots(1, TIMEOUT);
        var second = t.expireSnapshots(1, TIMEOUT); // idempotent: nothing more to expire
        verdict("expireTwice", "first=" + first.snapshotId() + " second=" + second.snapshotId()
                + " detail=" + second.detail());
        assertTrue(first.done() && second.done());
        assertEquals(secondDataSnapshot, first.snapshotId() == null ? "" : secondDataSnapshot);
        // the pre-compaction data snapshot has expired; only compaction/expire chain remains readable
        assertThrows(LakehouseException.class, () -> t.readAt(secondDataSnapshot));
        assertEquals(3, t.readHead().rows().size(), "head data still intact after repeated expiry");
    }
}
