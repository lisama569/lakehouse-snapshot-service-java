package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

public record SchemaChangeOutcome(boolean accepted, int newSchemaVersion, String snapshotId,
                                  RejectReason rejectReason, String detail) {
}
