package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.BatchStatus;
import com.github.lisama569.lakehousesnapshotservicejava.model.ReadResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.model.RowRecord;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 批次写入、upsert 与重复提交幂等性验证。 */
class WriteIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(WriteIdempotencyTest.class);

    @TempDir
    Path tempDir;

    private LakehouseTestSupport support;

    @BeforeEach
    void setUp() {
        support = new LakehouseTestSupport(tempDir, 5_000);
        support.createOrders("orders");
    }

    @Test
    void appendBatchThenReplayIsIdempotent() {
        var table = support.orders("orders");
        var req = LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.5),
                        LakehouseTestSupport.row(2, "b", 2.5)));

        WriteResult first = table.write(req);
        assertEquals(BatchStatus.COMMITTED, first.status());
        assertEquals(1L, first.committedOffset());
        assertEquals(1L, first.snapshotId());
        log.info("[test] first commit batchId={} snapshotId={} offset={}",
                first.batchId(), first.snapshotId(), first.committedOffset());

        // 同批次（含相同 expectedOffset）重复提交：回放首次结果，不产生重复数据。
        WriteResult replay = table.write(req);
        assertTrue(replay.replay());
        assertEquals(BatchStatus.COMMITTED, replay.status());
        assertEquals(1L, replay.snapshotId(), "重放必须返回首次快照标识");
        assertEquals(1L, replay.committedOffset());

        ReadResult read = table.read(null);
        assertEquals(2, read.rows().size(), "重复提交不能产生重复数据");
        log.info("[test] replay rows={} snapshotId={} offset={}",
                read.rows().size(), read.snapshotId(), read.offset());
    }

    @Test
    void sameBatchIdWithDifferentPayloadIsRejected() {
        var table = support.orders("orders");
        table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.5))));

        // 同 batchId、不同内容：即使位点对上，也必须区分性拒绝。
        LakehouseException ex = assertThrows(LakehouseException.class,
                () -> table.write(LakehouseTestSupport.append("orders", "b1", 1L,
                        List.of(LakehouseTestSupport.row(9, "different", 9.0)))));
        assertEquals(RejectReason.DUPLICATE_BATCH_CONFLICT, ex.getReason());
        log.info("[test] duplicate-batch-conflict reason={} message={}",
                ex.getReason(), ex.getMessage());
    }

    @Test
    void upsertUpdatesExistingRowAndIsIdempotent() {
        var table = support.orders("orders");
        table.write(LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.5))));
        var upsert = LakehouseTestSupport.upsert("orders", "u1", 1L,
                List.of(new RowRecord(java.util.Map.of("id", 1L, "name", "a2", "amount", 9.9)),
                        LakehouseTestSupport.row(2, "b", 2.0)));
        WriteResult r = table.write(upsert);
        assertEquals(BatchStatus.COMMITTED, r.status());
        ReadResult read = table.read(null);
        assertEquals(2, read.rows().size(), "upsert 不应产生重复行");
        assertEquals("a2", read.rows().get(0).values().get("name"));
        assertEquals(9.9, read.rows().get(0).values().get("amount"));

        WriteResult replay = table.write(upsert);
        assertTrue(replay.replay());
        assertEquals(2, table.read(null).rows().size());
        log.info("[test] upsert snapshotId={} rows={}", replay.snapshotId(),
                table.read(null).rows().size());
    }
}
