package com.github.lisama569.lakehousesnapshotservicejava.web.dto;

/** 统一错误响应：稳定错误码 + 可读消息。 */
public record ErrorResponse(String error, String message) {
}
