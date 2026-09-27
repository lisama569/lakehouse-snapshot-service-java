package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.ReadOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SnapshotReadService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotReadService.class);

    private final TableCatalog catalog;

    public SnapshotReadService(TableCatalog catalog) {
        this.catalog = catalog;
    }

    public ReadOutcome readHead(String table) {
        log.info("read head table={}", table);
        return catalog.open(table).readHead();
    }

    public ReadOutcome readAt(String table, String snapshotId) {
        log.info("read as-of table={} snapshot={}", table, snapshotId);
        return catalog.open(table).readAt(snapshotId);
    }
}
