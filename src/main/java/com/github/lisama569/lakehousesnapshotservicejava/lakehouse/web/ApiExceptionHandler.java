package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.web;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.RejectReason;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Translates distinct {@link RejectReason}s into distinct HTTP status codes. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(LakehouseException.class)
    public ResponseEntity<Map<String, Object>> handle(LakehouseException ex) {
        RejectReason reason = ex.reason();
        HttpStatus status = switch (reason) {
            case UNKNOWN_TABLE -> HttpStatus.NOT_FOUND;
            case UNKNOWN_SNAPSHOT -> HttpStatus.GONE;
            case TABLE_EXISTS -> HttpStatus.CONFLICT;
            case INVALID_OFFSET, PK_CONFLICT, TYPE_MISMATCH, BATCH_ID_REUSE,
                 SCHEMA_INCOMPATIBLE, BATCH_TIMEOUT, MAINTENANCE_BUSY, IO_FAILURE ->
                    HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(status)
                .body(Map.of("accepted", false, "rejectReason", reason.name(), "detail",
                        ex.getMessage() == null ? "" : ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(Map.of("accepted", false, "rejectReason", RejectReason.TYPE_MISMATCH.name(),
                        "detail", ex.getMessage() == null ? "" : ex.getMessage()));
    }
}
