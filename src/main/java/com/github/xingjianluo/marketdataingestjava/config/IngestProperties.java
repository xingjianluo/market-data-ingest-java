package com.github.xingjianluo.marketdataingestjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 摄取链路可配置参数。 */
@ConfigurationProperties(prefix = "ingest")
public class IngestProperties {

    /** 允许迟到窗口（毫秒）。 */
    private long allowedLatenessMs = 5000;

    /** 聚合桶大小（毫秒），默认 1 分钟。 */
    private long bucketSizeMs = 60_000;

    /** 本地快照文件位置。 */
    private String snapshotPath = "data/market-ingest-snapshot.json";

    public long getAllowedLatenessMs() { return allowedLatenessMs; }
    public void setAllowedLatenessMs(long v) { this.allowedLatenessMs = v; }

    public long getBucketSizeMs() { return bucketSizeMs; }
    public void setBucketSizeMs(long v) { this.bucketSizeMs = v; }

    public String getSnapshotPath() { return snapshotPath; }
    public void setSnapshotPath(String v) { this.snapshotPath = v; }
}
