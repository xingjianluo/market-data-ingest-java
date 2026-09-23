package com.github.xingjianluo.marketdataingestjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationBook;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedup.Deduplicator;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.log.EventLog;
import com.github.xingjianluo.marketdataingestjava.offset.CursorTracker;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotStore;
import com.github.xingjianluo.marketdataingestjava.window.LatenessPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.function.Supplier;

/** 本地装配：引擎、策略、重放服务均为进程内本地组件，不连接任何外部服务。 */
@Configuration
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {

    @Bean
    public LatenessPolicy latenessPolicy(IngestProperties properties) {
        return new LatenessPolicy(properties.getWindowSizeMillis(),
                properties.getAllowedLatenessMillis());
    }

    @Bean
    public IngestEngine ingestEngine(IngestProperties properties, LatenessPolicy policy) {
        return new IngestEngine(policy, Path.of(properties.getStateDir()));
    }

    /**
     * 重放服务：每次触发重放时在 {@code <stateDir>/replay/} 下构造空状态新引擎，
     * 读取现网 events.jsonl，不触碰现网快照。
     */
    @Bean
    public ReplayService replayService(IngestProperties properties, LatenessPolicy policy,
                                       IngestEngine engine) {
        Path stateDir = Path.of(properties.getStateDir());
        EventLog currentLog = new EventLog(stateDir.resolve("events.jsonl"));
        ObjectMapper mapper = new ObjectMapper();
        Supplier<IngestEngine> fresh = () -> {
            // 全量重放必须从空状态开始：清掉重放目录的残留快照与日志，绝不触碰现网目录
            Path replayDir = stateDir.resolve("replay");
            try {
                java.nio.file.Files.createDirectories(replayDir);
                java.nio.file.Files.deleteIfExists(replayDir.resolve("snapshot.json"));
                java.nio.file.Files.deleteIfExists(replayDir.resolve("events.jsonl"));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException("准备重放目录失败: " + replayDir, e);
            }
            return new IngestEngine(policy,
                    new EventCodec(mapper),
                    new CursorTracker(),
                    new Deduplicator(),
                    new AggregationBook(),
                    new SnapshotStore(replayDir.resolve("snapshot.json"), mapper),
                    new EventLog(replayDir.resolve("events.jsonl"), mapper),
                    true);
        };
        return new ReplayService(currentLog, fresh, engine);
    }

    static Path stateDir(IngestProperties properties) {
        return Path.of(properties.getStateDir());
    }
}
