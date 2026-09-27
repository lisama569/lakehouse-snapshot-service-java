package com.github.lisama569.lakehousesnapshotservicejava.model;

/**
 * 列定义。列以稳定的数字 id 标识，重命名只改 name 不改 id，
 * 因此旧数据文件不会因为列改名而错列。
 */
public record ColumnDef(int id, String name, DataType type) {
}
