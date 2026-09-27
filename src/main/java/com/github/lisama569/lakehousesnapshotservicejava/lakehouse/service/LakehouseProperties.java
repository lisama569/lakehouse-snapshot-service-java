package com.github.lisama569.lakehousesnapshotservicejava.lakehouse.service;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Local data directory and concurrency/timeout boundaries. */
@ConfigurationProperties(prefix = "lakehouse")
public class LakehouseProperties {

    /** Root data directory on local disk. */
    private String dataDir = "data/lakehouse";

    /** Max time to wait to acquire the serial commit lock. */
    private Duration commitTimeout = Duration.ofSeconds(5);

    /** Max time to wait for a maintenance (compact/expire) operation. */
    private Duration maintenanceTimeout = Duration.ofSeconds(30);

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public Duration getCommitTimeout() {
        return commitTimeout;
    }

    public void setCommitTimeout(Duration commitTimeout) {
        this.commitTimeout = commitTimeout;
    }

    public Duration getMaintenanceTimeout() {
        return maintenanceTimeout;
    }

    public void setMaintenanceTimeout(Duration maintenanceTimeout) {
        this.maintenanceTimeout = maintenanceTimeout;
    }
}
