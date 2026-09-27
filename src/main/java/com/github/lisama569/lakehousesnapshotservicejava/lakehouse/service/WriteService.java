package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchOutcome;
import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.BatchRequest;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Batch/upsert write entry point with the configured commit timeout boundary. */
@Service
public class WriteService {

    private static final Logger log = LoggerFactory.getLogger(WriteService.class);

    private final TableCatalog catalog;
    private final LakehouseProperties props;

    public WriteService(TableCatalog catalog, LakehouseProperties props) {
        this.catalog = catalog;
        this.props = props;
    }

    public BatchOutcome submit(BatchRequest request, Duration timeout) {
        Duration effective = timeout != null ? timeout : props.getCommitTimeout();
        String table = request == null ? null : request.table();
        log.info("write submit table={} batch={} mode={} expectedOffset={} timeoutMs={}",
                table, request == null ? null : request.batchId(),
                request == null ? null : request.mode(),
                request == null ? null : request.expectedOffset(), effective.toMillis());
        return catalog.open(table).submit(request, effective);
    }
}
