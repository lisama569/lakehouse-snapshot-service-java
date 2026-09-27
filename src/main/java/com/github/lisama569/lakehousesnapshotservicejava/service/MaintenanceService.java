package com.github.lisama569.lakehousesnapshotservicejava.service;

import com.github.lisama569.lakehousesnapshotservicejava.config.LakehouseProperties;
import com.github.lisama569.lakehousesnapshotservicejava.engine.TableCatalog;
import com.github.lisama569.lakehousesnapshotservicejava.model.CleanupResult;
import com.github.lisama569.lakehousesnapshotservicejava.model.WriteResult;
import org.springframework.stereotype.Service;

/** 小文件合并与过期快照清理维护入口，与写入共用同一把提交锁。 */
@Service
public class MaintenanceService {

    private final TableCatalog catalog;
    private final LakehouseProperties properties;

    public MaintenanceService(TableCatalog catalog, LakehouseProperties properties) {
        this.catalog = catalog;
        this.properties = properties;
    }

    public WriteResult compact(String table, Long timeoutMs) {
        long timeout = timeoutMs != null && timeoutMs > 0
                ? timeoutMs : properties.getMaintenanceTimeoutMs();
        return catalog.get(table).compact(timeout);
    }

    public CleanupResult expire(String table, Integer retain, Long timeoutMs) {
        int retainCount = retain != null && retain >= 1
                ? retain : properties.getDefaultRetainSnapshots();
        long timeout = timeoutMs != null && timeoutMs > 0
                ? timeoutMs : properties.getMaintenanceTimeoutMs();
        return catalog.get(table).expireSnapshots(retainCount, timeout);
    }
}
