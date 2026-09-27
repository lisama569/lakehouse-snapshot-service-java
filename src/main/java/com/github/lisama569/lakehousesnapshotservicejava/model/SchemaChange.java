package com.github.lisama569.lakehousesnapshotservicejava.model;

/**
 * 模式变更：新增 / 重命名 / 删除列。
 * ADD：column=新列名，dataType 必填，defaultValue 可空（默认 null）。
 * RENAME：column=旧列名，renameTo=新列名。
 * DROP：column=被删除列名。
 */
public record SchemaChange(
        ChangeType op,
        String column,
        String renameTo,
        DataType dataType,
        Object defaultValue
) {
    public enum ChangeType {
        ADD,
        RENAME,
        DROP
    }
}
