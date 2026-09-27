package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/**
 * 读取结果。timeTravel=true 表示读取的是历史快照；
 * defaultFilledColumns 列出因旧数据缺少新列而被填默认值（null）的列。
 */
public record ReadResult(
        String table,
        long snapshotId,
        int schemaVersion,
        long offset,
        boolean timeTravel,
        List<String> defaultFilledColumns,
        List<RowRecord> rows
) {
}
