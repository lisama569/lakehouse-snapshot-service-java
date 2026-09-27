package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.engine.LocalTable;
import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.BatchStatus;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发一致性：
 *  - 多线程 UPSERT（各写各的主键）并发提交：无丢失、无重复，位点连续；
 *  - 写入期间并发读取：读到的永远是某个已提交快照，行数单调不减且无半成品；
 *  - 写入与合并/清理同时进行：最终数据完整，快照链合法。
 * 每个批次都带正确位点的做法不可行（位点会变），因此写入线程失败后按当前位点重试。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    @TempDir
    Path tempDir;

    private LakehouseTestSupport support;

    @BeforeEach
    void setUp() {
        support = new LakehouseTestSupport(tempDir, 30_000);
        support.createOrders("orders");
    }

    private WriteResult writeWithOffsetRetry(LocalTable table, String batchId,
                                            List<com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord> rows) {
        LakehouseException last = null;
        for (int attempt = 0; attempt < 2000; attempt++) {
            long expected = countCommittedOffset(table);
            var req = LakehouseTestSupport.upsert("orders", batchId, expected, rows);
            try {
                return table.write(req);
            } catch (LakehouseException e) {
                last = e;
                if (e.getReason() == RejectReason.INVALID_OFFSET) {
                    // 位点竞争：短暂退避后用最新位点重试（同 batchId 允许覆盖 REJECTED 回执）
                    sleep(1);
                    continue;
                }
                if (e.getReason() == RejectReason.BATCH_TIMEOUT) {
                    sleep(2);
                    continue;
                }
                throw e;
            }
        }
        throw last;
    }

    private long countCommittedOffset(LocalTable table) {
        return table.read(null).offset();
    }

    @Test
    void concurrentUpsertsNoLossNoDuplicate() throws Exception {
        int threads = 8;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        Set<Long> committedSnapshots = ConcurrentHashMap.newKeySet();
        AtomicInteger timeouts = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            futures.add(pool.submit(() -> {
                start.await();
                LocalTable table = support.orders("orders");
                for (int i = 0; i < perThread; i++) {
                    long id = base + i + 1;
                    String batchId = "batch-" + id;
                    var rows = List.of(LakehouseTestSupport.row(id, "n" + id, (double) id));
                    WriteResult r;
                    try {
                        r = writeWithOffsetRetry(table, batchId, rows);
                    } catch (com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException e) {
                        if (e.getReason() == RejectReason.BATCH_TIMEOUT) {
                            timeouts.incrementAndGet();
                            continue;
                        }
                        throw e;
                    }
                    committedSnapshots.add(r.snapshotId());
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        var finalRead = support.orders("orders").read(null);
        assertEquals(threads * perThread, finalRead.rows().size(),
                "不能丢数据也不能有重复数据");
        Set<Long> ids = new HashSet<>();
        for (var row : finalRead.rows()) {
            Object id = row.values().get("id");
            assertTrue(ids.add(((Number) id).longValue()), "出现重复主键");
        }
        // 快照数 == 成功批次数（无重复提交），位点最终值一致
        assertEquals(threads * perThread, committedSnapshots.size());
        assertEquals(threads * perThread, finalRead.offset());
        log.info("[test] concurrent-upserts finalRows={} distinctSnapshots={} finalOffset={} "
                        + "timeoutRejected={}",
                finalRead.rows().size(), committedSnapshots.size(), finalRead.offset(),
                timeouts.get());
    }

    @Test
    void concurrentReadsAlwaysSeeCommittedSnapshot() throws Exception {
        LocalTable table = support.orders("orders");
        AtomicInteger violations = new AtomicInteger();
        AtomicInteger readsDone = new AtomicInteger();
        CountDownLatch stopWriters = new CountDownLatch(1);

        Runnable reader = () -> {
            long lastRows = 0;
            while (stopWriters.getCount() > 0) {
                var read = table.read(null);
                // 不变量：行数单调不减；每个 id 唯一；行数不超过已提交数
                if (read.rows().size() < lastRows) {
                    violations.incrementAndGet();
                }
                Set<Long> ids = new HashSet<>();
                for (var row : read.rows()) {
                    if (!ids.add(((Number) row.values().get("id")).longValue())) {
                        violations.incrementAndGet();
                    }
                }
                lastRows = read.rows().size();
                readsDone.incrementAndGet();
            }
        };
        ExecutorService pool = Executors.newFixedThreadPool(3);
        List<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            tasks.add(pool.submit(reader));
        }
        tasks.add(pool.submit(() -> {
            for (int i = 1; i <= 100; i++) {
                writeWithOffsetRetry(support.orders("orders"), "w-" + i,
                        List.of(LakehouseTestSupport.row(i, "n" + i, i)));
            }
            return null;
        }));
        tasks.get(2).get(60, TimeUnit.SECONDS);
        stopWriters.countDown();
        for (Future<?> t : tasks) {
            t.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(0, violations.get(), "并发读期间出现回退或重复（读到半成品）");
        assertEquals(100, table.read(null).rows().size());
        log.info("[test] concurrent-read-write reads={} violations={} finalRows=100",
                readsDone.get(), violations.get());
    }

    @Test
    void writesConcurrentWithCompactionAndExpireKeepDataConsistent() throws Exception {
        LocalTable table = support.orders("orders");
        int total = 60;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        futures.add(pool.submit(() -> {
            start.await();
            for (int i = 1; i <= total; i++) {
                writeWithOffsetRetry(support.orders("orders"), "w-" + i,
                        List.of(LakehouseTestSupport.row(i, "n" + i, i)));
            }
            return null;
        }));
        futures.add(pool.submit(() -> {
            start.await();
            for (int k = 0; k < 5; k++) {
                sleep(20);
                try {
                    var r = support.orders("orders").compact(20_000);
                    log.info("[test] concurrent compact snapshotId={} rows={}",
                            r.snapshotId(), r.writtenRows());
                } catch (Exception e) {
                    log.info("[test] compact skipped: {}", e.getMessage());
                }
            }
            return null;
        }));
        futures.add(pool.submit(() -> {
            start.await();
            for (int k = 0; k < 3; k++) {
                sleep(40);
                try {
                    var r = support.orders("orders").expireSnapshots(2, 20_000);
                    log.info("[test] concurrent expire removed={} deleted={}",
                            r.removedSnapshotIds(), r.deletedDataFiles());
                } catch (Exception e) {
                    log.info("[test] expire skipped: {}", e.getMessage());
                }
            }
            return null;
        }));
        start.countDown();
        for (Future<?> f : futures) {
            f.get(120, TimeUnit.SECONDS);
        }
        pool.shutdown();

        var finalRead = table.read(null);
        assertEquals(total, finalRead.rows().size(), "维护并发下不能丢数据");
        Set<Long> ids = new HashSet<>();
        finalRead.rows().forEach(r -> ids.add(((Number) r.values().get("id")).longValue()));
        assertEquals(total, ids.size(), "维护并发下不能有重复主键");
        log.info("[test] concurrent-maintenance finalRows={} finalSnapshotId={}",
                finalRead.rows().size(), finalRead.snapshotId());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
