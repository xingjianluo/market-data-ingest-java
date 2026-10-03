package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 整体重放核对：把链路已收下的原始事件，从一个干净空白状态完整重放一遍，
 * 逐条把重放结论与首次处理结论比对，输出统计与不一致明细。
 *
 * <p>语义约束：
 * <ul>
 *   <li>重放必须从干净起点开始：由 {@link IngestPipeline#freshScratchPipeline()}
 *       构造全新内存状态，绝不复用线上管道当前内存状态，也不读写快照文件。</li>
 *   <li>干净状态下各源已处理位点恒为 {@code -1}；若调用方显式给出的续处理起始位置
 *       不是 {@code -1}（更靠前或更靠后都一样），一律 {@link ReplayRefusedException}
 *       明确拒绝，不能从中间悄悄接上去。</li>
 *   <li>逐条重放时仍带当日首次处理时的期望位点，于是“首次处理拒绝/接受”的每条结论
 *       都能在干净状态下被原样复现；不一致逐条记录，最后再逐字段比对最终聚合。</li>
 * </ul>
 */
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    /** 干净状态下任何来源的已处理位点。 */
    static final long CLEAN_OFFSET = -1L;

    private final IngestPipeline live;

    public ReplayService(IngestPipeline live) {
        this.live = live;
    }

    /** 从干净起点全量重放日志并逐条核对。 */
    public ReplayReport replayFromScratch() {
        return replayFromScratch(Map.of());
    }

    /**
     * 从干净起点全量重放，并显式给出各源续处理起始位置。
     * 干净状态已处理位点恒为 -1，任何 != -1 的位置（更靠前或更靠后）都拒绝。
     */
    public ReplayReport replayFromScratch(Map<String, Long> requestedStartOffsets) {
        Map<String, Long> requested = requestedStartOffsets == null ? Map.of() : requestedStartOffsets;
        requested.forEach((source, offset) -> {
            if (offset == null || offset != CLEAN_OFFSET) {
                throw new ReplayRefusedException(String.format(
                        "scratch replay refused: requested start offset for source='%s' is %s, "
                                + "but a clean pipeline only has committed offset %d; "
                                + "必须从干净起点（-1）整体重放，不允许从中间接上",
                        source, offset, CLEAN_OFFSET));
            }
        });

        // 先在线上锁内取一份按接收顺序的日志拷贝；重放过程绝不触碰 live 的内存状态。
        List<JournaledEvent> journal = live.journal().copyAll();

        IngestPipeline scratch = live.freshScratchPipeline();
        List<ReplayMismatch> mismatches = new ArrayList<>();
        int matched = 0;

        log.info("[重放] 从干净空白状态开始整体重放：共 {} 条原始事件，latiness={}ms bucket={}ms",
                journal.size(), live.allowedLatenessMs(), live.bucketSizeMs());

        for (JournaledEvent entry : journal) {
            IngestResult replayed = scratch.resumeIngest(entry.rawJson(), entry.expectedOffsets());
            if (replayed.status() == entry.originalStatus()) {
                matched++;
                log.info("[重放] #{}/{} source={} eventId={} 结论一致: {}",
                        entry.ordinal() + 1, journal.size(),
                        entry.source(), entry.eventId(), replayed.status());
            } else {
                ReplayMismatch mismatch = new ReplayMismatch(
                        entry.ordinal(), entry.source(), entry.eventId(),
                        entry.originalStatus(), replayed.status(),
                        entry.originalReason(), replayed.reason());
                mismatches.add(mismatch);
                log.warn("[重放] #{}/{} 不一致 source={} eventId={} 期望={} 实际={}",
                        entry.ordinal() + 1, journal.size(), entry.source(), entry.eventId(),
                        entry.originalStatus(), replayed.status());
            }
        }

        // 最终聚合逐字段核对（只由被接受的事件集合决定，多源同号也必须一致）
        List<AggregateView> liveViews = live.aggregates();
        List<AggregateView> replayViews = scratch.aggregates();
        boolean aggregatesMatch = liveViews.equals(replayViews);
        if (!aggregatesMatch) {
            log.warn("[重放] 最终聚合不一致: liveBuckets={} replayBuckets={}",
                    liveViews.size(), replayViews.size());
        }

        ReplayReport report = new ReplayReport(journal.size(), matched,
                mismatches.size(), aggregatesMatch, List.copyOf(mismatches));
        log.info("[重放] 完成：重放={} 一致={} 不一致={} 聚合逐字段一致={}",
                report.totalReplayed(), report.matched(), report.mismatched(),
                report.aggregatesMatch());
        return report;
    }
}
