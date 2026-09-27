package com.github.lisama569.lakehousesnapshotservicejava.web;

import com.github.lisama569.lakehousesnapshotservicejava.exception.LakehouseException;
import com.github.lisama569.lakehousesnapshotservicejava.model.RejectReason;
import com.github.lisama569.lakehousesnapshotservicejava.web.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 把稳定错误码映射为可区分的 HTTP 状态与响应体。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(LakehouseException.class)
    public ResponseEntity<ErrorResponse> handle(LakehouseException ex) {
        RejectReason reason = ex.getReason();
        HttpStatus status = switch (reason) {
            case TABLE_NOT_FOUND, SNAPSHOT_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case TABLE_ALREADY_EXISTS -> HttpStatus.CONFLICT;
            case BATCH_TIMEOUT, MAINTENANCE_TIMEOUT -> HttpStatus.REQUEST_TIMEOUT;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(new ErrorResponse(reason.name(), ex.getMessage()));
    }
}
