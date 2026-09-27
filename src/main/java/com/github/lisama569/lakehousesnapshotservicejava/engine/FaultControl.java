package com.github.lisama569.lakehousesnapshotservicejava.engine;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 故障注入开关（仅测试用）：开启后下一次提交在“数据文件已写、
 * 状态尚未更新”的位置抛出，验证崩溃恢复与重放的幂等性。
 * 通过 HTTP /api/tables/{table}/fault 或测试直接设置。
 */
@Component
public class FaultControl {

    private final AtomicBoolean crashBeforeCommit = new AtomicBoolean(false);

    public void armCrashBeforeCommit() {
        crashBeforeCommit.set(true);
    }

    public boolean consumeCrashBeforeCommit() {
        return crashBeforeCommit.compareAndSet(true, false);
    }

    public boolean isArmed() {
        return crashBeforeCommit.get();
    }

    public void clear() {
        crashBeforeCommit.set(false);
    }
}
