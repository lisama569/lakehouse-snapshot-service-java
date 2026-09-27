package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteRequest;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteMode;
import com.github.lisama569.lakehousesnapshotservicejava.storage.LockManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 各类拒绝原因必须可区分：位点非法、主键冲突、类型不匹配、未知列、缺主键、批次超时。 */
class RejectionReasonTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionReasonTest.class);

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
    void invalidOffsetIsRejected() {
        var table = support.orders("orders");
        // 当前位点 0，提交 5 -> INVALID_OFFSET
        expectReason(RejectReason.INVALID_OFFSET, () -> table.write(
                LakehouseTestSupport.append("orders", "b1", 5L,
                        List.of(LakehouseTestSupport.row(1, "a", 1.0)))));

        // 合法提交一次后，仍用过时位点 0 -> INVALID_OFFSET
        table.write(LakehouseTestSupport.append("orders", "b2", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        expectReason(RejectReason.INVALID_OFFSET, () -> table.write(
                LakehouseTestSupport.append("orders", "b3", 0L,
                        List.of(LakehouseTestSupport.row(2, "b", 2.0)))));
    }

    @Test
    void pkConflictIsRejectedInAppend() {
        var table = support.orders("orders");
        table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        expectReason(RejectReason.PK_CONFLICT, () -> table.write(
                LakehouseTestSupport.append("orders", "b2", 1L,
                        List.of(LakehouseTestSupport.row(1, "dup", 2.0)))));
        // 同批次内主键重复
        expectReason(RejectReason.PK_CONFLICT, () -> table.write(
                LakehouseTestSupport.append("orders", "b3", 1L,
                        List.of(LakehouseTestSupport.row(2, "x", 1.0),
                                LakehouseTestSupport.row(2, "y", 2.0)))));
    }

    @Test
    void typeMismatchIsRejected() {
        var table = support.orders("orders");
        expectReason(RejectReason.TYPE_MISMATCH, () -> table.write(
                new WriteRequest("orders", "b1", WriteMode.APPEND, 0L, 0L,
                        List.of(new RowRecord(Map.of("id", "not-a-number", "name", "a",
                                "amount", 1.0))))));
        expectReason(RejectReason.TYPE_MISMATCH, () -> table.write(
                new WriteRequest("orders", "b2", WriteMode.APPEND, 0L, 0L,
                        List.of(new RowRecord(Map.of("id", 1, "name", 123,
                                "amount", 1.0))))));
    }

    @Test
    void unknownColumnAndMissingPrimaryKeyAreRejected() {
        var table = support.orders("orders");
        expectReason(RejectReason.UNKNOWN_COLUMN, () -> table.write(
                new WriteRequest("orders", "b1", WriteMode.APPEND, 0L, 0L,
                        List.of(new RowRecord(Map.of("id", 1, "name", "a", "amount", 1.0,
                                "ghost", true))))));
        expectReason(RejectReason.MISSING_PRIMARY_KEY, () -> table.write(
                new WriteRequest("orders", "b2", WriteMode.APPEND, 0L, 0L,
                        List.of(new RowRecord(Map.of("name", "a", "amount", 1.0))))));
    }

    @Test
    void rejectedBatchIsMarkedAndRetryWithCorrectOffsetSucceeds() {
        var table = support.orders("orders");
        expectReason(RejectReason.INVALID_OFFSET, () -> table.write(
                LakehouseTestSupport.append("orders", "b1", 9L,
                        List.of(LakehouseTestSupport.row(1, "a", 1.0)))));
        // FAILED/REJECTED 批次允许用相同 batchId、修正位点后重试并成功，位点只前进一次。
        var ok = table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        assertEquals(1L, ok.committedOffset());
        assertEquals(1, table.read(null).rows().size());
    }

    @Test
    void batchTimeoutWhenLockBusy() throws Exception {
        // 用独立 LockManager 模拟“锁被其它执行方长时间占用”。
        LockManager other = new LockManager();
        AutoCloseable held = other.acquire("orders", support.store.lockFile("orders"), 1_000,
                RejectReason.MAINTENANCE_TIMEOUT);
        try {
            var table = support.orders("orders");
            long start = System.currentTimeMillis();
            LakehouseException ex = assertThrows(LakehouseException.class, () -> table.write(
                    new WriteRequest("orders", "b1", WriteMode.APPEND, 0L, 100L,
                            List.of(LakehouseTestSupport.row(1, "a", 1.0)))));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals(RejectReason.BATCH_TIMEOUT, ex.getReason());
            assertTrue(elapsed < 2_000, "必须在超时边界附近返回，elapsed=" + elapsed);
            log.info("[test] batch-timeout reason={} elapsedMs={} detail={}",
                    ex.getReason(), elapsed, ex.getMessage());
        } finally {
            held.close();
        }
        // 锁释放后同批次可成功（超时未提交）。
        var ok = support.orders("orders").write(
                new WriteRequest("orders", "b1", WriteMode.APPEND, 0L, 1_000L,
                        List.of(LakehouseTestSupport.row(1, "a", 1.0))));
        assertEquals(1L, ok.committedOffset());
    }
}
