package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/** Runtime exception carrying a distinct {@link RejectReason}. */
public class LakehouseException extends RuntimeException {

    private final RejectReason reason;

    public LakehouseException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
