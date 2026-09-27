package com.github.lisama569.lakehousesnapshotservicejava.tests;

import com.github.lisama569.lakehousesnapshotservicejava.model.BatchStatus;
import com.github.lisama569.lakehousesnapshotservicejava.model.DataType;
import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 模拟进程重启：用同一数据目录重建引擎，状态、回执、快照与模式必须完整恢复。 */
class PersistenceRestartTest {

    private static final Logger log = LoggerFactory.getLogger(PersistenceRestartTest.class);

    @TempDir
    Path tempDir;

    @Test
    void stateSurvivesRestartAndReplayStillIdempotent() {
        LakehouseTestSupport first = new LakehouseTestSupport(tempDir, 5_000);
        first.createOrders("orders");
        var t1 = first.orders("orders");
        var req = LakehouseTestSupport.append("orders", "b1", 0L,
                List.of(LakehouseTestSupport.row(1, "a", 1.0)));
        WriteResult committed = t1.write(req);
        t1.evolve(new SchemaChange(SchemaChange.ChangeType.ADD, "tag", null,
                DataType.STRING, null));
        log.info("[test] before-restart snapshotId={} schemaVersion=2", committed.snapshotId());

        // 重建引擎（进程重启），不新建表
        LakehouseTestSupport second = new LakehouseTestSupport(tempDir, 5_000);
        var t2 = second.orders("orders");
        assertEquals(2, t2.schema().version(), "模式版本必须持久化恢复");
        assertEquals(1, t2.read(null).rows().size());

        WriteResult replay = t2.write(req);
        assertTrue(replay.replay());
        assertEquals(BatchStatus.COMMITTED, replay.status());
        assertEquals(committed.snapshotId(), replay.snapshotId(),
                "重启后同一批次仍命中回执，返回首次快照");
        assertEquals(1L, replay.committedOffset());
        assertEquals(1, t2.read(null).rows().size());

        // 位点继续生效：用过时位点写入被拒，正确位点写入成功
        assertThrows(com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException.class,
                () -> t2.write(LakehouseTestSupport.append("orders", "b2", 0L,
                        List.of(LakehouseTestSupport.row(2, "b", 2.0)))));
        var ok = t2.write(LakehouseTestSupport.append("orders", "b2", 1L,
                List.of(LakehouseTestSupport.row(2, "b", 2.0))));
        assertEquals(2L, ok.committedOffset());
        assertEquals(2, t2.read(null).rows().size());
        log.info("[test] after-restart replaySnapshotId={} finalOffset=2 finalRows=2",
                replay.snapshotId());
    }
}
