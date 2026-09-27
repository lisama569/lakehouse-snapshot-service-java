package com.github.lisama569.lakehousesnapshotservicejava.exception;

import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;

/** 所有可区分的业务拒绝 / 失败都以该异常携带稳定错误码抛出。 */
public class LakehouseException extends RuntimeException {

    private final RejectReason reason;

    public LakehouseException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
