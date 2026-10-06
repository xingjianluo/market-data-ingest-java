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

    /**
     * 干净起点：一条都还没处理过的位置（流水账序号 0）。
     * 核对只允许从这里发起；任何其它续处理位置都会被明确拒绝。
     */
    public static final long CLEAN_START_POSITION = 0L;

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
     * 发起整体重放核对（带续处理位置校验）。
     *
     * <p>核对只允许从干净起点（{@link #CLEAN_START_POSITION}，一条都还没处理过的位置）
     * 发起：每次核对都新建一整套与当前内存完全无关的管道组件从头重放，
     * 绝不从中间接上去继续算。因此：
     * <ul>
     *   <li>{@code resumeFromPosition} 缺省（null）或等于干净起点 → 接受，完整重放；</li>
     *   <li>比干净起点靠前（回退）→ 拒绝，reason 含 {@code regression refused}；</li>
     *   <li>比干净起点靠后（跳跃，会漏计前面的历史事件）→ 拒绝，reason 含 {@code gap refused}。</li>
     * </ul>
     * 拒绝时一条都不会重放，结论为
     * {@link ReplayVerificationStatus#REJECTED_OFFSET_REGRESSION}，
     * 与正常核对通过（VERIFIED）及摄取侧的重复/其它拒绝一眼可分。
     *
     * @param journal            摄取流水账快照（调用方已拷贝，核对过程不回写）
     * @param resumeFromPosition 调用方要求“从哪里接着处理”的位置；null 表示干净起点
     */
    public ReplayVerificationResult verify(List<RecordedIngestion> journal, Long resumeFromPosition) {
        long position = resumeFromPosition == null ? CLEAN_START_POSITION : resumeFromPosition;
        if (position != CLEAN_START_POSITION) {
            String reason = position < CLEAN_START_POSITION
                    ? String.format("offset regression refused: requested=%d < clean-start=%d "
                            + "(核对只能从干净起点 %d 发起，不允许回退)",
                            position, CLEAN_START_POSITION, CLEAN_START_POSITION)
                    : String.format("offset gap refused: requested=%d > clean-start=%d "
                            + "(从中间接上去会漏计前 %d 条历史事件，核对只能从干净起点 %d 发起)",
                            position, CLEAN_START_POSITION, position, CLEAN_START_POSITION);
            log.warn("[重放核对] 拒绝发起: {}", reason);
            return ReplayVerificationResult.rejected(reason);
        }
        return ReplayVerificationResult.verified(verify(journal));
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
