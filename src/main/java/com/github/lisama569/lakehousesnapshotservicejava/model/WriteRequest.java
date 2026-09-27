package com.github.lisama569.lakehousesnapshotservicejava.model;

import java.util.List;

/**
 * 写入请求。
 * expectedOffset：调用方认为当前已提交位点（已提交批次数），必须与服务端一致；
 * timeoutMs：等待提交锁的超时边界，<=0 时使用服务端默认值。
 */
public record WriteRequest(
        String table,
        String batchId,
        WriteMode mode,
        long expectedOffset,
        long timeoutMs,
        List<RowRecord> rows
) {
}
