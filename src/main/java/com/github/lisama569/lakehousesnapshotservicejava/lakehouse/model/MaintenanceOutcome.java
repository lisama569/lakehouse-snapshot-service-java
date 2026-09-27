package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

import java.util.List;

public record MaintenanceOutcome(boolean done, String operation, String snapshotId,
                                 int filesBefore, int filesAfter, List<String> removedFiles,
                                 RejectReason rejectReason, String detail) {
}
