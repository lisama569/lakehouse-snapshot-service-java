package com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse;

import static com.github.lisama569.lakehousesnapshotservicejava.tests.lakehouse.LakehouseTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.MaintenanceOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ReadOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LakehouseTable;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.LocalFileStore;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage.TablePaths;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConcurrencyTest {

    @TempDir
    Path dir;

    @Test
    void concurrentAppendsSerializeWithoutLossOrDuplication() throws Exception {
        LakehouseTable t = createStandard(dir, "t");
        int n = 20;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            long pk = i + 1;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        BatchOutcome out = t.submit(batch("t", "b-" + pk, t.nextOffset(), WriteMode.APPEND,
                                row(1, pk, 2, "n" + pk)), Duration.ofSeconds(5));
                        if (out.accepted()) {
                            accepted.incrementAndGet();
                            break;
                        }
                        if (out.rejectReason() != RejectReason.INVALID_OFFSET) {
                            rejected.incrementAndGet();
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        var rows = t.readHead().rows();
        Set<Object> pks = new HashSet<>();
        rows.forEach(r -> pks.add(r.values().get("id")));
        System.out.println("[VERDICT:concurrentAppends] accepted=" + accepted.get()
                + " rejected=" + rejected.get() + " rows=" + rows.size() + " distinctPks=" + pks.size()
                + " finalOffset=" + t.nextOffset());
        assertEquals(n, accepted.get());
        assertEquals(0, rejected.get());
        assertEquals(n, rows.size(), "no lost or duplicated rows");
        assertEquals(n, pks.size());
        assertEquals(n, t.nextOffset());
    }

    @Test
    void concurrentReplayOfSameBatchCommitsExactlyOnce() throws Exception {
        LakehouseTable t = createStandard(dir, "t");
        int n = 10;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<BatchOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return t.submit(batch("t", "same-batch", 0, WriteMode.APPEND,
                        row(1, 42L, 2, "x")), Duration.ofSeconds(5));
            }));
        }
        start.countDown();
        int committed = 0;
        int replayed = 0;
        String commitSnapshot = null;
        for (Future<BatchOutcome> f : futures) {
            BatchOutcome o = f.get();
            assertTrue(o.accepted(), "all calls succeed: one commit + n-1 replays");
            if (o.replay()) {
                replayed++;
            } else {
                committed++;
                commitSnapshot = o.snapshotId();
            }
        }
        pool.shutdown();
        System.out.println("[VERDICT:concurrentReplay] committed=" + committed + " replayed=" + replayed
                + " snapshot=" + commitSnapshot + " rows=" + t.readHead().rows().size());
        assertEquals(1, committed);
        assertEquals(n - 1, replayed);
        assertEquals(1, t.readHead().rows().size());
    }

    @Test
    void readsDuringCompactionAlwaysSeeACompleteConsistentTable() throws Exception {
        LakehouseTable t = createStandard(dir, "t");
        for (int i = 0; i < 30; i++) {
            var out = t.submit(batch("t", "b-" + i, i, WriteMode.UPSERT,
                    row(1, (long) (i % 7), 2, "n" + i, 3, (double) i)), Duration.ofSeconds(5));
            assertTrue(out.accepted());
        }
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger readsChecked = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        futures.add(pool.submit(() -> {
            try {
                start.await();
                MaintenanceOutcome o = t.compact(Duration.ofSeconds(10));
                System.out.println("[VERDICT:readDuringCompact] compact done=" + o.done()
                        + " snapshot=" + o.snapshotId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        for (int r = 0; r < 3; r++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 20; i++) {
                        ReadOutcome read = t.readHead();
                        Set<Object> pks = new HashSet<>();
                        read.rows().forEach(row -> pks.add(row.values().get("id")));
                        assertEquals(7, read.rows().size(),
                                "partial read at snapshot " + read.snapshotId()
                                        + " rows=" + read.rows().size());
                        assertEquals(7, pks.size(), "duplicate pk during concurrent compaction");
                        readsChecked.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        System.out.println("[VERDICT:readDuringCompact] consistentReads=" + readsChecked.get());
        assertEquals(7, t.readHead().rows().size());
    }

    @Test
    void timeoutBoundariesAreReportedDistinctly() throws Exception {
        LocalFileStore store = store(dir);
        LakehouseTable t = LakehouseTable.create(store, new TablePaths(dir.resolve("t")),
                "t", standardColumns(), "id");

        // Block inside a commit while its write lock is held, then contend with a writer
        // and a maintenance operation using short timeouts.
        CountDownLatch insideCommit = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        store.failureHook = path -> {
            if (path.getFileName().toString().startsWith("commit-")) {
                insideCommit.countDown();
                try {
                    releaseCommit.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return false;
        };

        ExecutorService pool = Executors.newFixedThreadPool(3);
        Future<?> committer = pool.submit(() ->
                t.submit(batch("t", "holder", 0, WriteMode.APPEND, row(1, 1L)), Duration.ofSeconds(10)));
        insideCommit.await();

        Future<BatchOutcome> writer = pool.submit(() ->
                t.submit(batch("t", "contender", 1, WriteMode.APPEND, row(1, 2L)), Duration.ofMillis(100)));
        Future<MaintenanceOutcome> compactor =
                pool.submit(() -> t.compact(Duration.ofMillis(100)));

        BatchOutcome writeOutcome = writer.get();
        MaintenanceOutcome compactOutcome = compactor.get();
        System.out.println("[VERDICT:timeoutBoundaries] write=" + writeOutcome + " compact=" + compactOutcome);
        assertEquals(RejectReason.BATCH_TIMEOUT, writeOutcome.rejectReason());
        assertEquals(RejectReason.MAINTENANCE_BUSY, compactOutcome.rejectReason());

        releaseCommit.countDown();
        committer.get();
        pool.shutdown();
        store.failureHook = p -> false;

        BatchOutcome after = t.submit(batch("t", "next", 1, WriteMode.APPEND, row(1, 2L)),
                Duration.ofSeconds(5));
        System.out.println("[VERDICT:timeoutBoundaries] afterContention=" + after);
        assertTrue(after.accepted());
    }
}
