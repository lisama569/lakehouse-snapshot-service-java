package com.github.lisama569.lakehousesnapshotservicejava.web.dto;

import com.github.lisama569.lakehousesnapshotservicejava.model.SchemaChange;

import java.util.List;

/** 模式演进请求：按顺序原子应用一组变更。 */
public record EvolveRequest(List<SchemaChange> changes) {
}
