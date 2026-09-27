package com.github.lisama569.lakehousesnapshotservicejava.model;

/** 写入模式：APPEND 仅追加（同主键视为冲突），UPSERT 按主键合并。 */
public enum WriteMode {
    APPEND,
    UPSERT
}
