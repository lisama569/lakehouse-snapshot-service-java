package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import com.github.lisama569.lakehousesnapshotservicejava.lakehouse.model.MaintenanceOutcome;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MaintenanceService {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);

    private final TableCatalog catalog;
    private final LakehouseProperties props;

    public MaintenanceService(TableCatalog catalog, LakehouseProperties props) {
        this.catalog = catalog;
        this.props = props;
    }

    public MaintenanceOutcome compact(String table, Duration timeout) {
        Duration effective = timeout != null ? timeout : props.getMaintenanceTimeout();
        log.info("compact table={} timeoutMs={}", table, effective.toMillis());
        return catalog.open(table).compact(effective);
    }

    public MaintenanceOutcome expireSnapshots(String table, int retainCount, Duration timeout) {
        Duration effective = timeout != null ? timeout : props.getMaintenanceTimeout();
        log.info("expire table={} retain={} timeoutMs={}", table, retainCount, effective.toMillis());
        return catalog.open(table).expireSnapshots(retainCount, effective);
    }
}
