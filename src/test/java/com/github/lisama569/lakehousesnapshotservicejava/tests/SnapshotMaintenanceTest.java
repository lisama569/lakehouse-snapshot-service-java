package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.CleanupResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 不可变快照、时间旅行、小文件合并与过期快照清理验证。 */
class SnapshotMaintenanceTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotMaintenanceTest.class);

    @TempDir
    Path tempDir;

    private LakehouseTestSupport support;

    @BeforeEach
    void setUp() {
        support = new LakehouseTestSupport(tempDir, 5_000);
        support.createOrders("orders");
    }

    @Test
    void snapshotsAreImmutableAndTimeTravelWorks() {
        var table = support.orders("orders");
        WriteResult s1 = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        WriteResult s2 = table.write(LakehouseTestSupport.append("orders", "b2", 1L,
                List.of(LakehouseTestSupport.row(2, "b", 2.0))));
        WriteResult s3 = table.write(LakehouseTestSupport.append("orders", "b3", 2L,
                List.of(LakehouseTestSupport.row(3, "c", 3.0))));

        assertEquals(1, table.read(s1.snapshotId()).rows().size());
        assertEquals(2, table.read(s2.snapshotId()).rows().size());
        assertEquals(3, table.read(s3.snapshotId()).rows().size());

        LakehouseException ex = assertThrows(LakehouseException.class,
                () -> table.read(999L));
        assertEquals(RejectReason.SNAPSHOT_NOT_FOUND, ex.getReason());
        log.info("[test] time-travel snapshotIds={},{},{} rows=1,2,3; missingSnapshot rejected={}",
                s1.snapshotId(), s2.snapshotId(), s3.snapshotId(), ex.getReason());
    }

    @Test
    void compactRewritesSingleFileWithoutLosingData() {
        var table = support.orders("orders");
        table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        table.write(LakehouseTestSupport.append("orders", "b2", 1L,
                List.of(LakehouseTestSupport.row(2, "b", 2.0))));
        var compacted = table.compact(5_000);
        log.info("[test] compact snapshotId={} writtenRows={}",
                compacted.snapshotId(), compacted.writtenRows());

        // 合并后数据完整、无重复
        var read = table.read(null);
        assertEquals(2, read.rows().size());
        assertEquals(2L, read.rows().get(1).values().get("id"));

        // 旧快照依然可读（旧文件未删除）
        assertEquals(1, table.read(1L).rows().size());

        long dataFiles;
        try (var s = Files.list(support.store.dataDir("orders"))) {
            dataFiles = s.filter(Files::isRegularFile).count();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertTrue(dataFiles >= 3, "旧文件保留，清理前不删除: " + dataFiles);
    }

    @Test
    void expireRemovesOldSnapshotsAndOnlyOrphanFiles() {
        var table = support.orders("orders");
        var s1 = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        var s2 = table.write(LakehouseTestSupport.append("orders", "b2", 1L,
                List.of(LakehouseTestSupport.row(2, "b", 2.0))));
        var s3 = table.write(LakehouseTestSupport.append("orders", "b3", 2L,
                List.of(LakehouseTestSupport.row(3, "c", 3.0))));

        // retain=2：保留 s2/s3，清理 s1。s1 的增量文件被 s2/s3 引用链引用，绝不能删。
        CleanupResult result = table.expireSnapshots(2, 5_000);
        assertEquals(List.of(s1.snapshotId()), result.removedSnapshotIds());
        assertTrue(result.deletedDataFiles().isEmpty(),
                "被后续快照引用的数据文件不能删除");
        assertEquals(2, result.retainedSnapshots());
        log.info("[test] expire removedSnapshots={} deletedFiles={} retained={}",
                result.removedSnapshotIds(), result.deletedDataFiles(),
                result.retainedSnapshots());

        // 被清理快照时间旅行失败，保留快照可读且数据完整
        assertEquals(RejectReason.SNAPSHOT_NOT_FOUND,
                assertThrows(LakehouseException.class,
                        () -> table.read(s1.snapshotId())).getReason());
        assertEquals(3, table.read(s3.snapshotId()).rows().size());

        // 合并后再清理，旧的增量文件成为孤儿才会被删除
        var compacted = table.compact(5_000);
        CleanupResult second = table.expireSnapshots(1, 5_000);
        // 仅剩 compact 快照：只引用合并文件，旧增量文件应被删除，当前数据仍完整
        assertEquals(3, table.read(compacted.snapshotId()).rows().size());
        assertFalse(second.deletedDataFiles().isEmpty());
        log.info("[test] expire-after-compact removed={} deletedFiles={}",
                second.removedSnapshotIds(), second.deletedDataFiles());
    }

    @Test
    void maintenanceTimeoutIsReportedSeparately() throws Exception {
        var otherLock = new com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager()
                .acquire("orders", support.store.lockFile("orders"), 1_000,
                        RejectReason.MAINTENANCE_TIMEOUT);
        try {
            LakehouseException ex = assertThrows(LakehouseException.class,
                    () -> support.orders("orders").compact(100L));
            assertEquals(RejectReason.MAINTENANCE_TIMEOUT, ex.getReason());
            log.info("[test] maintenance-timeout reason={} detail={}", ex.getReason(),
                    ex.getMessage());
        } finally {
            otherLock.close();
        }
    }
}
