package com.github.xingjianluo.marketdataingestjava.pipeline;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.codec.InvalidEventException;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.replay.RecordedIngestion;
import com.github.xingjianluo.marketdataingestjava.state.PipelineState;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 摄取与聚合主管道。所有状态判定在单一临界区内完成，保证多源并发下结论一致。
 *
 * <p>幂等口径：幂等键为 (source, eventId)。多来源各自独立编号，
 * 不同来源的相同 eventId（哪怕业务时间也相同）是各自独立的业务事件，分别计入；
 * 只有同一来源重发同一 eventId 才判 DUPLICATE。两种结论在 reason 中显式区分。
 *
 * <p>判定顺序（前一步拒绝则后续不再执行，且不落任何状态）：
 * <ol>
 *   <li>结构解析：非法 → REJECTED_INVALID</li>
 *   <li>重放位点对齐：期望位点与已提交位点不一致 → REJECTED_OFFSET_REGRESSION</li>
 *   <li>重复：(source, eventId) 已计入 → DUPLICATE（同一事件重放天然幂等，先于位点判定）</li>
 *   <li>位点回退：{@code seq <= committedOffset} 且为新事件 → REJECTED_OFFSET_REGRESSION</li>
 *   <li>迟到：{@code eventTime < watermark - allowedLateness} → REJECTED_LATE</li>
 *   <li>接受：去重登记 → 聚合并入 → 水位推进 → 位点提交</li>
 * </ol>
 *
 * <p>每条报文（含被拒绝的）都会连同首次处理结论写入摄取流水账 journal，
 * 顺序与临界区处理顺序一致，供“干净状态整体重放核对”使用。
 */
public class IngestPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestPipeline.class);

    private final EventCodec codec;
    private final DedupTable dedupTable;
    private final WatermarkManager watermarkManager;
    private final CheckpointManager checkpointManager;
    private final AggregationTable aggregationTable;
    private final SnapshotRepository snapshotRepository;
    private final Object lock = new Object();

    /** 摄取流水账：按实际处理顺序记录 (原始报文, 首次结论)。 */
    private final List<RecordedIngestion> journal = new ArrayList<>();

    public IngestPipeline(EventCodec codec,
                          DedupTable dedupTable,
                          WatermarkManager watermarkManager,
                          CheckpointManager checkpointManager,
                          AggregationTable aggregationTable,
                          SnapshotRepository snapshotRepository) {
        this.codec = codec;
        this.dedupTable = dedupTable;
        this.watermarkManager = watermarkManager;
        this.checkpointManager = checkpointManager;
        this.aggregationTable = aggregationTable;
        this.snapshotRepository = snapshotRepository;
    }

    /** 摄取单条原始 JSON 事件。 */
    public IngestResult ingest(String rawJson) {
        return doIngest(rawJson, Map.of());
    }

    /**
     * 断点续处理/重放入口：若 expectedOffsets 中给出该源的期望位点，
     * 则必须与已提交位点严格相等；回退（小）或跳跃（大，会漏计）都明确拒绝。
     */
    public IngestResult resumeIngest(String rawJson, Map<String, Long> expectedOffsets) {
        return doIngest(rawJson, expectedOffsets == null ? Map.of() : expectedOffsets);
    }

    private IngestResult doIngest(String rawJson, Map<String, Long> expectedOffsets) {
        // 1) 解析在锁外完成（纯函数）；非法事件不触碰任何状态。
        final MarketEvent event;
        try {
            event = codec.decode(rawJson);
        } catch (InvalidEventException ex) {
            log.warn("[判定] INVALID eventId={} reason='{}' raw={}",
                    ex.getRawEventId(), ex.getMessage(), abbreviate(rawJson));
            IngestResult result = new IngestResult(IngestStatus.REJECTED_INVALID,
                    ex.getRawEventId(), ex.getMessage(), Long.MIN_VALUE,
                    CheckpointManager.NO_OFFSET);
            record(rawJson, expectedOffsets, result);
            return result;
        }

        synchronized (lock) {
            // 判定与落状态在同一临界区；流水账追加也在锁内，
            // 因此 journal 顺序 == 实际处理顺序，重放可复现。
            IngestResult result = judgeAndApply(event, expectedOffsets);
            record(rawJson, expectedOffsets, result);
            return result;
        }
    }

    /** 临界区内的判定与状态变更（不加锁，调用方必须持有 lock）。 */
    private IngestResult judgeAndApply(MarketEvent event, Map<String, Long> expectedOffsets) {
        long committed = checkpointManager.committedOffsetOf(event.source());
        long watermark = watermarkManager.watermarkOf(event.source());

        // 2) 重放/续处理：严格位点对齐校验
        Long expected = expectedOffsets.get(event.source());
        if (expected != null && expected != committed) {
            String reason = expected < committed
                    ? String.format("offset regression refused: requested=%d <= committed=%d (重放必须从已提交位点 %d 开始)",
                            expected, committed, committed)
                    : String.format("offset gap refused: requested=%d > committed=%d (跳跃前进会漏计事件)",
                            expected, committed);
            log.warn("[判定] OFFSET_REGRESSION eventId={} source={} seq={} {}",
                    event.eventId(), event.source(), event.sequence(), reason);
            return new IngestResult(IngestStatus.REJECTED_OFFSET_REGRESSION,
                    event.eventId(), reason, watermark, committed);
        }

        // 3) 重复：幂等键为 (source, eventId)。同一来源重发同一 eventId 才判重，
        //    先于位点回退判定（同一事件重放天然幂等）。
        if (dedupTable.isDuplicate(event.source(), event.eventId())) {
            String reason = String.format(
                    "duplicate: eventId='%s' already aggregated from source='%s' (同一来源重发，不计入)",
                    event.eventId(), event.source());
            log.info("[判定] DUPLICATE eventId={} source={} seq={} (同源重发，不计入)",
                    event.eventId(), event.source(), event.sequence());
            return new IngestResult(IngestStatus.DUPLICATE,
                    event.eventId(), reason, watermark, committed);
        }

        // 4) 位点回退：新事件却落在已提交位点，明确拒绝
        if (checkpointManager.isRegression(event.source(), event.sequence())) {
            String reason = String.format(
                    "offset regression: seq=%d <= committedOffset=%d, source='%s'",
                    event.sequence(), committed, event.source());
            log.warn("[判定] OFFSET_REGRESSION eventId={} source={} seq={} committed={}",
                    event.eventId(), event.source(), event.sequence(), committed);
            return new IngestResult(IngestStatus.REJECTED_OFFSET_REGRESSION,
                    event.eventId(), reason, watermark, committed);
        }

        // 5) 迟到
        if (watermarkManager.isLate(event.source(), event.eventTime())) {
            String reason = String.format(
                    "too late: eventTime=%d < watermark=%d - allowedLateness=%d, source='%s'",
                    event.eventTime(), watermark, watermarkManager.allowedLatenessMs(),
                    event.source());
            log.warn("[判定] LATE eventId={} source={} seq={} eventTime={} watermark={} allowedLateness={}",
                    event.eventId(), event.source(), event.sequence(),
                    event.eventTime(), watermark, watermarkManager.allowedLatenessMs());
            return new IngestResult(IngestStatus.REJECTED_LATE,
                    event.eventId(), reason, watermark, committed);
        }

        // 6) 接受：登记 -> 聚合 -> 水位 -> 位点（同一临界区，要么全做要么全不做）
        //    若该 eventId 已由其它来源计入，结论中显式标注“跨源独立事件”，
        //    与“同源重发 DUPLICATE”一眼区分。
        var otherSources = dedupTable.sourcesOf(event.eventId());
        boolean crossSourceIndependent = !otherSources.isEmpty();
        dedupTable.markSeen(event.source(), event.eventId());
        aggregationTable.apply(event);
        long newWatermark = watermarkManager.advance(event.source(), event.eventTime());
        checkpointManager.commit(event.source(), event.sequence());

        String reason = crossSourceIndependent
                ? String.format("aggregated into bucket (independent event: eventId='%s' also seen from other sources %s, 各源独立编号互不去重)",
                        event.eventId(), otherSources)
                : "aggregated into bucket";
        log.info("[判定] ACCEPTED eventId={} source={} seq={} eventTime={} v={} symbol={} price={} qty={} venue={}{} -> watermark={} committed={}",
                event.eventId(), event.source(), event.sequence(), event.eventTime(),
                event.schemaVersion(), event.symbol(), event.price(), event.quantity(),
                event.venue(), crossSourceIndependent ? " [跨源独立事件, 其它来源=" + otherSources + "]" : "",
                newWatermark, event.sequence());
        return new IngestResult(IngestStatus.ACCEPTED, event.eventId(),
                reason, newWatermark, event.sequence());
    }

    private void record(String rawJson, Map<String, Long> expectedOffsets, IngestResult result) {
        synchronized (journal) {
            journal.add(new RecordedIngestion(journal.size(), rawJson,
                    Map.copyOf(expectedOffsets), result));
        }
    }

    /** 摄取流水账（只读副本），按实际处理顺序排列，供整体重放核对。 */
    public List<RecordedIngestion> journal() {
        synchronized (journal) {
            return List.copyOf(journal);
        }
    }

    /** 原子持久化当前全量状态（先在锁内取一致快照，IO 在锁外执行）。 */
    public void snapshot() {
        final PipelineState state;
        synchronized (lock) {
            state = new PipelineState(
                    System.currentTimeMillis(),
                    watermarkManager.allowedLatenessMs(),
                    aggregationTable.bucketSizeMs(),
                    dedupTable.snapshot(),
                    watermarkManager.snapshotWatermarks(),
                    checkpointManager.snapshotOffsets(),
                    aggregationTable.allViews());
        }
        snapshotRepository.save(state);
        log.info("[快照] 已持久化: seenPairs={} sources={} buckets={} savedAt={}",
                state.seenEventIds().values().stream().mapToInt(java.util.Set::size).sum(),
                state.committedOffsets().size(),
                state.buckets().size(), state.savedAtMillis());
    }

    /** 从持久化状态恢复（重建全部内存状态）；窗口/桶参数与当前配置不一致时拒绝恢复。 */
    public void restore() {
        PipelineState state = snapshotRepository.load();
        if (state == null) {
            log.info("[恢复] 无本地快照，按空状态启动");
            return;
        }
        synchronized (lock) {
            if (state.allowedLatenessMs() != watermarkManager.allowedLatenessMs()
                    || state.bucketSizeMs() != aggregationTable.bucketSizeMs()) {
                throw new IllegalStateException(String.format(
                        "snapshot parameters mismatch: snapshot(lateness=%d,bucket=%d) vs current(lateness=%d,bucket=%d)",
                        state.allowedLatenessMs(), state.bucketSizeMs(),
                        watermarkManager.allowedLatenessMs(), aggregationTable.bucketSizeMs()));
            }
            dedupTable.restore(state.seenEventIds());
            watermarkManager.restore(state.watermarks());
            checkpointManager.restore(state.committedOffsets());
            aggregationTable.restoreBuckets(state.buckets());
            log.info("[恢复] 已加载快照: seenPairs={} watermarks={} offsets={} buckets={} savedAt={}",
                    state.seenEventIds().values().stream().mapToInt(java.util.Set::size).sum(),
                    state.watermarks(),
                    state.committedOffsets(), state.buckets().size(), state.savedAtMillis());
        }
    }

    public List<AggregateView> aggregates() {
        synchronized (lock) {
            return aggregationTable.allViews();
        }
    }

    public long committedOffset(String source) {
        synchronized (lock) {
            return checkpointManager.committedOffsetOf(source);
        }
    }

    public long watermark(String source) {
        synchronized (lock) {
            return watermarkManager.watermarkOf(source);
        }
    }

    public long allowedLatenessMs() {
        return watermarkManager.allowedLatenessMs();
    }

    public long bucketSizeMs() {
        return aggregationTable.bucketSizeMs();
    }

    /** 供重放核对器复用同一解析器（解析器无状态）。 */
    public EventCodec codec() {
        return codec;
    }

    private static String abbreviate(String raw) {
        if (raw == null) {
            return "null";
        }
        return raw.length() <= 160 ? raw : raw.substring(0, 160) + "...";
    }
}
