package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model;

/** A requested schema evolution action. */
public sealed interface SchemaChange
        permits SchemaChange.AddColumn, SchemaChange.RenameColumn, SchemaChange.DropColumn {

    record AddColumn(String name, ColumnType type, boolean nullable) implements SchemaChange {
    }

    record RenameColumn(String from, String to) implements SchemaChange {
    }

    record DropColumn(String name) implements SchemaChange {
    }
}
