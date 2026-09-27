package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse.LakehouseTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LocalFileStore;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FailureRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void failureDuringDataStagingRejectsAndWritesNothing() {
        LocalFileStore store = store(dir);
        LakehouseTable t = LakehouseTable.create(store,
                new com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.TablePaths(dir.resolve("t")),
                "t", standardColumns(), "id");
        // fail exactly when the batch's data file is about to be published
        store.failureHook = path -> path.getFileName().toString().startsWith("seq-");

        BatchOutcome out = t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        verdict("stagingFailure", out);
        assertFalse(out.accepted());
        assertEquals(RejectReason.IO_FAILURE, out.rejectReason());

        store.failureHook = p -> false;
        assertEquals(0, t.readHead().rows().size(), "no half-published rows visible");
        assertEquals(0, t.nextOffset(), "offset did not advance");

        // same batch can be retried cleanly and is not treated as a replay
        BatchOutcome retry = t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        verdict("stagingFailureRetry", retry);
        assertTrue(retry.accepted());
        assertEquals(1, t.readHead().rows().size());
    }

    @Test
    void failureAfterCommitRecordButBeforeHeadIsRecoveredAsAborted() {
        LocalFileStore store = store(dir);
        LakehouseTable t = LakehouseTable.create(store,
                new com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.TablePaths(dir.resolve("t")),
                "t", standardColumns(), "id");
        // fail on the head pointer publish: commit record may exist but head stays at s1
        store.failureHook = path -> path.getFileName().toString().equals("head.json");

        BatchOutcome out = t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        verdict("headPublishFailure", out);
        assertFalse(out.accepted());
        store.failureHook = p -> false;

        // reopen: the orphaned commit record + staged data are treated as aborted and reclaimed
        LakehouseTable reopened = recover(dir, "t");
        System.out.println("[VERDICT:crashRecovery] headAfterRecover=" + reopened.headSnapshotId()
                + " rows=" + reopened.readHead().rows().size() + " offset=" + reopened.nextOffset());
        assertEquals("s1", reopened.headSnapshotId(), "head never advanced, so transaction is aborted");
        assertEquals(0, reopened.readHead().rows().size());
        assertEquals(0, reopened.nextOffset());

        var files = new LocalFileStore(dir, mapper()).list(reopened.paths().dataDir());
        assertTrue(files.isEmpty(), "staged orphan data file was reclaimed during recovery, files=" + files);
    }

    @Test
    void recoveryAfterCommitsRebuildsOffsetAndIdempotencyLedger() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        t.submit(batch("t", "b-2", 1, WriteMode.APPEND, row(1, 2L, 2, "b")), TIMEOUT);

        LakehouseTable reopened = recover(dir, "t");
        assertEquals(2, reopened.nextOffset());
        assertEquals(2, reopened.readHead().rows().size());

        BatchOutcome replay = reopened.submit(
                batch("t", "b-2", 1, WriteMode.APPEND, row(1, 2L, 2, "b")), TIMEOUT);
        verdict("replayAfterRecover", replay);
        assertTrue(replay.accepted() && replay.replay(), "idempotency ledger survives restart");
        assertEquals(2, reopened.readHead().rows().size());

        BatchOutcome gap = reopened.submit(batch("t", "b-3", 9, WriteMode.APPEND, row(1, 3L)), TIMEOUT);
        assertEquals(RejectReason.INVALID_OFFSET, gap.rejectReason());
    }

    @Test
    void validationFailureStagesNoFile() {
        LakehouseTable t = createStandard(dir, "t");
        t.submit(batch("t", "b-1", 0, WriteMode.APPEND, row(1, 1L, 2, "a")), TIMEOUT);
        var bad = t.submit(batch("t", "b-x", 1, WriteMode.APPEND, row(1, 2L, 2, 42)), TIMEOUT);
        assertEquals(RejectReason.TYPE_MISMATCH, bad.rejectReason());
        var files = new LocalFileStore(dir, mapper()).list(t.paths().dataDir());
        assertEquals(1, files.size(), "rejected batch never staged a data file: " + files);
        assertEquals(1, t.nextOffset());
    }
}
