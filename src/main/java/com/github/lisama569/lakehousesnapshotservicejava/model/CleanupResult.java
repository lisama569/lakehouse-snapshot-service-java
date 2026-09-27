package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/** 快照清理结果：列出被移除的快照与被删除的数据文件，便于审计与测试断言。 */
public record CleanupResult(
        String table,
        int retainedSnapshots,
        List<Long> removedSnapshotIds,
        List<String> deletedDataFiles,
        long headSnapshotId
) {
}
