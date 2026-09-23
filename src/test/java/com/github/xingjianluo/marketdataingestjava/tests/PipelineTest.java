package com.github.xingjianluo.marketdataingestjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.state.FileSnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

abstract class PipelineTest {

    static final long LATENESS = 5_000;
    static final long BUCKET = 60_000;

    @TempDir
    Path tempDir;

    IngestPipeline newPipeline() {
        ObjectMapper mapper = new ObjectMapper();
        return new IngestPipeline(
                new EventCodec(mapper),
                new DedupTable(),
                new WatermarkManager(LATENESS),
                new CheckpointManager(),
                new AggregationTable(BUCKET),
                new FileSnapshotRepository(tempDir.resolve("snapshot.json"), mapper));
    }
}
