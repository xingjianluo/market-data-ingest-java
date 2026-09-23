package com.github.xingjianluo.marketdataingestjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 摄取链路本地配置。 */
@ConfigurationProperties(prefix = "market.ingest")
public class IngestProperties {

    private long windowSizeMillis = 60_000L;
    private long allowedLatenessMillis = 5_000L;
    private String stateDir = "data/ingest";

    public long getWindowSizeMillis() { return windowSizeMillis; }
    public void setWindowSizeMillis(long windowSizeMillis) { this.windowSizeMillis = windowSizeMillis; }
    public long getAllowedLatenessMillis() { return allowedLatenessMillis; }
    public void setAllowedLatenessMillis(long allowedLatenessMillis) { this.allowedLatenessMillis = allowedLatenessMillis; }
    public String getStateDir() { return stateDir; }
    public void setStateDir(String stateDir) { this.stateDir = stateDir; }
}
