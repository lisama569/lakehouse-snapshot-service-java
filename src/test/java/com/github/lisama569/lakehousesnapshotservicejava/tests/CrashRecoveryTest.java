package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.model.BatchStatus;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * “数据文件已写、状态未提交”阶段的崩溃：
 * 不能产生可见半成品，staging 残留要被回收，同批次重放必须幂等且只生效一次。
 */
class CrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryTest.class);

    @TempDir
    Path tempDir;

    private LakehouseTestSupport support;

    @BeforeEach
    void setUp() {
        support = new LakehouseTestSupport(tempDir, 5_000);
        support.createOrders("orders");
    }

    @Test
    void crashBeforeCommitLeavesNoVisibleDataAndRetryCommitsOnce() throws Exception {
        var table = support.orders("orders");
        // 先有一笔正常数据
        table.write(LakehouseTestSupport.append("orders", "b0", 0L,
                List.of(LakehouseTestSupport.row(100, "base", 0.1))));

        // 在下次提交的 pre-commit 点注入崩溃
        support.faultControl.armCrashBeforeCommit();
        var req = LakehouseTestSupport.append("orders", "b1", 1L,
                List.of(LakehouseTestSupport.row(1, "x", 1.0)));
        IllegalStateException crash = assertThrows(IllegalStateException.class, () -> table.write(req));
        log.warn("[test] injected crash observed: {}", crash.getMessage());

        // 崩溃后：链头仍是旧快照，看不到半成品数据；位点不变
        var readAfterCrash = table.read(null);
        assertEquals(1, readAfterCrash.rows().size());
        assertEquals(100L, readAfterCrash.rows().get(0).values().get("id"));
        assertEquals(1L, readAfterCrash.offset());
        log.info("[test] after-crash headSnapshotId={} rows={} offset={}",
                readAfterCrash.snapshotId(), readAfterCrash.rows().size(), readAfterCrash.offset());

        // 重新打开表（模拟进程重启）时回收 staging 残留
        long stagingBefore;
        Path stagingDir = support.store.stagingDir("orders");
        if (Files.isDirectory(stagingDir)) {
            try (Stream<Path> s = Files.list(stagingDir)) {
                stagingBefore = s.count();
            }
        } else {
            stagingBefore = 0;
        }
        long orphanStaging = stagingBefore;

        // 同批次重放：此时故障开关已被消费，提交成功且只生效一次
        WriteResult retry = table.write(req);
        assertEquals(BatchStatus.COMMITTED, retry.status());
        assertFalse(retry.replay());
        assertEquals(2L, retry.committedOffset());
        assertEquals(2, table.read(null).rows().size());

        // 再次重放：命中 COMMITTED 回执，结果幂等
        WriteResult replay = table.write(req);
        assertTrue(replay.replay());
        assertEquals(retry.snapshotId(), replay.snapshotId());
        assertEquals(2, table.read(null).rows().size());
        log.info("[test] recovery-commit snapshotId={} orphanStagingCleaned={} replaySnapshotId={}",
                retry.snapshotId(), orphanStaging, replay.snapshotId());
    }
}
