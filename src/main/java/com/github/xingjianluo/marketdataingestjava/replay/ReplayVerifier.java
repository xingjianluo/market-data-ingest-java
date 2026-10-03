package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.state.InMemorySnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 整体重放核对器：把链路已收下的原始事件（摄取流水账），
 * 从一个干净空白的状态完整重放一遍，逐条比对重放结论与首次处理结论。
 *
 * <p>关键约束：
 * <ul>
 *   <li>每次核对都新建一整套管道组件（去重表/水位/位点/聚合/快照仓库全部全新），
 *       绝不复用、也不读取当前内存状态，保证从干净起点开始；</li>
 *   <li>重放顺序 = 流水账顺序 = 首次处理的实际串行顺序，
 *       由于全部判定都是该顺序的确定性函数，结论应逐条一致；</li>
 *   <li>比对维度：status + reason + 判定时水位 + 判定时已提交位点，逐字段比对。</li>
 * </ul>
 */
public class ReplayVerifier {

    private static final Logger log = LoggerFactory.getLogger(ReplayVerifier.class);

    private final EventCodec codec;
    private final long allowedLatenessMs;
    private final long bucketSizeMs;

    public ReplayVerifier(EventCodec codec, long allowedLatenessMs, long bucketSizeMs) {
        this.codec = codec;
        this.allowedLatenessMs = allowedLatenessMs;
        this.bucketSizeMs = bucketSizeMs;
    }

    /** 以给定管道的配置构造核对器（复用其无状态解析器与窗口/桶参数）。 */
    public static ReplayVerifier forPipeline(IngestPipeline pipeline) {
        return new ReplayVerifier(pipeline.codec(), pipeline.allowedLatenessMs(),
                pipeline.bucketSizeMs());
    }

    /**
     * 从干净空白状态完整重放 journal，逐条比对首次结论，返回统计与不一致明细。
     */
    public ReplayReport verify(List<RecordedIngestion> journal) {
        // 干净起点：全部组件全新构造，不触碰任何现有状态。
        IngestPipeline fresh = new IngestPipeline(
                codec, new DedupTable(),
                new WatermarkManager(allowedLatenessMs), new CheckpointManager(),
                new AggregationTable(bucketSizeMs), new InMemorySnapshotRepository());

        List<ReplayMismatch> mismatches = new ArrayList<>();
        long matched = 0;
        for (RecordedIngestion entry : journal) {
            IngestResult expected = entry.firstResult();
            // 以与首次完全相同的输入重放：普通摄取走 ingest，
            // 携带期望位点的续处理投递走 resumeIngest，位点对齐判定才能复现。
            IngestResult actual = entry.expectedOffsets().isEmpty()
                    ? fresh.ingest(entry.rawJson())
                    : fresh.resumeIngest(entry.rawJson(), entry.expectedOffsets());
            if (sameConclusion(expected, actual)) {
                matched++;
            } else {
                ReplayMismatch mismatch = new ReplayMismatch(
                        entry.journalIndex(), expected.eventId(),
                        expected.status(), actual.status(),
                        expected.reason(), actual.reason(),
                        expected.sourceWatermark(), actual.sourceWatermark(),
                        expected.sourceCommittedOffset(), actual.sourceCommittedOffset());
                mismatches.add(mismatch);
                log.warn("[重放核对] 不一致: {}", mismatch);
            }
        }

        ReplayReport report = new ReplayReport(journal.size(), matched,
                journal.size() - matched, List.copyOf(mismatches));
        log.info("[重放核对] {} (干净状态重放 {} 条)", report.summary(), journal.size());
        return report;
    }

    private static boolean sameConclusion(IngestResult a, IngestResult b) {
        return a.status() == b.status()
                && a.sourceWatermark() == b.sourceWatermark()
                && a.sourceCommittedOffset() == b.sourceCommittedOffset()
                && Objects.equals(a.reason(), b.reason());
    }
}
