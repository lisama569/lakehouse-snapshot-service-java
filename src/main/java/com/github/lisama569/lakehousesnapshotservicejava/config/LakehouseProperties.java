package com.github.lisama569.lakehousesnapshotservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 本地湖仓服务配置，全部有默认值，开箱即用、不依赖外部服务。 */
@ConfigurationProperties(prefix = "lakehouse")
public class LakehouseProperties {

    /** 数据根目录（state 与不可变数据文件都落在本地）。 */
    private String dataDir = "build/lakehouse-data";

    /** 批次等待提交锁的默认超时（毫秒）。 */
    private long defaultBatchTimeoutMs = 10_000;

    /** 合并 / 清理等维护操作等待锁的默认超时（毫秒）。 */
    private long maintenanceTimeoutMs = 30_000;

    /** 清理快照时默认保留的快照数量。 */
    private int defaultRetainSnapshots = 3;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public long getDefaultBatchTimeoutMs() {
        return defaultBatchTimeoutMs;
    }

    public void setDefaultBatchTimeoutMs(long defaultBatchTimeoutMs) {
        this.defaultBatchTimeoutMs = defaultBatchTimeoutMs;
    }

    public long getMaintenanceTimeoutMs() {
        return maintenanceTimeoutMs;
    }

    public void setMaintenanceTimeoutMs(long maintenanceTimeoutMs) {
        this.maintenanceTimeoutMs = maintenanceTimeoutMs;
    }

    public int getDefaultRetainSnapshots() {
        return defaultRetainSnapshots;
    }

    public void setDefaultRetainSnapshots(int defaultRetainSnapshots) {
        this.defaultRetainSnapshots = defaultRetainSnapshots;
    }
}
