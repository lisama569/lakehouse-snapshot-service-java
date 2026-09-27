package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.storage;

import java.nio.file.Path;

/** Resolves on-disk paths for one table. */
public record TablePaths(Path tableDir) {

    public Path metadataDir() {
        return tableDir.resolve("metadata");
    }

    public Path dataDir() {
        return tableDir.resolve("data");
    }

    public Path headFile() {
        return metadataDir().resolve("head.json");
    }

    public Path schemaFile(int version) {
        return metadataDir().resolve("schema-v" + version + ".json");
    }

    /** Durable commit record (the authoritative source of truth on recovery). */
    public Path commitFile(long seq) {
        return metadataDir().resolve("commit-" + seq + ".json");
    }

    public Path dataFile(String fileName) {
        return dataDir().resolve(fileName);
    }
}
