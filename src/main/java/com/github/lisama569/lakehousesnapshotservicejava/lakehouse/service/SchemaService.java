package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChange;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.SchemaChangeOutcome;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SchemaService {

    private static final Logger log = LoggerFactory.getLogger(SchemaService.class);

    private final TableCatalog catalog;
    private final LakehouseProperties props;

    public SchemaService(TableCatalog catalog, LakehouseProperties props) {
        this.catalog = catalog;
        this.props = props;
    }

    public SchemaChangeOutcome evolve(String table, SchemaChange change, Duration timeout) {
        Duration effective = timeout != null ? timeout : props.getCommitTimeout();
        log.info("schema evolve table={} timeoutMs={}", table, effective.toMillis());
        return catalog.open(table).evolve(change, effective);
    }
}
