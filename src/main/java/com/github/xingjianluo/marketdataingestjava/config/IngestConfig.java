package com.github.xingjianluo.marketdataingestjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import com.github.xingjianluo.marketdataingestjava.state.FileSnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/** 摄取链路装配（全部本地内存 + 本地文件，无外部服务）。 */
@Configuration
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {

    @Bean
    public EventCodec eventCodec(ObjectMapper objectMapper) {
        return new EventCodec(objectMapper);
    }

    @Bean
    public DedupTable dedupTable() {
        return new DedupTable();
    }

    @Bean
    public WatermarkManager watermarkManager(IngestProperties props) {
        return new WatermarkManager(props.getAllowedLatenessMs());
    }

    @Bean
    public CheckpointManager checkpointManager() {
        return new CheckpointManager();
    }

    @Bean
    public AggregationTable aggregationTable(IngestProperties props) {
        return new AggregationTable(props.getBucketSizeMs());
    }

    @Bean
    public SnapshotRepository snapshotRepository(IngestProperties props, ObjectMapper objectMapper) {
        return new FileSnapshotRepository(Path.of(props.getSnapshotPath()), objectMapper);
    }

    @Bean
    public IngestPipeline ingestPipeline(EventCodec codec,
                                         DedupTable dedupTable,
                                         WatermarkManager watermarkManager,
                                         CheckpointManager checkpointManager,
                                         AggregationTable aggregationTable,
                                         SnapshotRepository snapshotRepository) {
        return new IngestPipeline(codec, dedupTable, watermarkManager, checkpointManager,
                aggregationTable, snapshotRepository);
    }

    @Bean
    public ReplayService replayService(IngestPipeline ingestPipeline) {
        return new ReplayService(ingestPipeline);
    }
}
