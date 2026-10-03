package com.github.xingjianluo.marketdataingestjava.pipeline;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.codec.InvalidEventException;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.dedupe.EventKey;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.replay.EventJournal;
import com.github.xingjianluo.marketdataingestjava.replay.JournaledEvent;
import com.github.xingjianluo.marketdataingestjava.state.PipelineState;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 摄取与聚合主管道。所有状态判定在单一临界区内完成，保证多源并发下结论一致。
 *
 * <p>判定顺序（前一步拒绝则后续不再执行，且不落任何状态）：
 * <ol>
 *   <li>结构解析：非法 → REJECTED_INVALID</li>
 *   <li>重放位点对齐：期望位点与已提交位点不一致 → REJECTED_OFFSET_REGRESSION</li>
 *   <li>重复：eventId 已计入 → DUPLICATE（同一事件重放天然幂等，先于位点判定）</li>
 *   <li>位点回退：{@code seq <= committedOffset} 且为新 eventId → REJECTED_OFFSET_REGRESSION</li>
 *   <li>迟到：{@code eventTime < watermark - allowedLateness} → REJECTED_LATE</li>
 *   <li>接受：去重登记 → 聚合并入 → 水位推进 → 位点提交</li>
 * </ol>
 */
public class IngestPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestPipeline.class);

    private final EventCodec codec;
    private final DedupTable dedupTable;
    private final WatermarkManager watermarkManager;
    private final CheckpointManager checkpointManager;
    private final AggregationTable aggregationTable;
    private final SnapshotRepository snapshotRepository;
    private final EventJournal journal;
    private final Object lock = new Object();

    public IngestPipeline(EventCodec codec,
                          DedupTable dedupTable,
                          WatermarkManager watermarkManager,
                          CheckpointManager checkpointManager,
                          AggregationTable aggregationTable,
                          SnapshotRepository snapshotRepository) {
        this(codec, dedupTable, watermarkManager, checkpointManager, aggregationTable,
                snapshotRepository, new EventJournal());
    }

    public IngestPipeline(EventCodec codec,
                          DedupTable dedupTable,
                          WatermarkManager watermarkManager,
                          CheckpointManager checkpointManager,
                          AggregationTable aggregationTable,
                          SnapshotRepository snapshotRepository,
                          EventJournal journal) {
        this.codec = codec;
        this.dedupTable = dedupTable;
        this.watermarkManager = watermarkManager;
        this.checkpointManager = checkpointManager;
        this.aggregationTable = aggregationTable;
        this.snapshotRepository = snapshotRepository;
        this.journal = journal;
    }

    /** 构造一个配置与本管道完全相同、状态干净空白的新管道（整体重放用）。 */
    public IngestPipeline freshScratchPipeline() {
        return new IngestPipeline(
                codec,
                new DedupTable(),
                new WatermarkManager(watermarkManager.allowedLatenessMs()),
                new CheckpointManager(),
                new AggregationTable(aggregationTable.bucketSizeMs()),
                new com.github.xingjianluo.marketdataingestjava.replay.NoopSnapshotRepository(),
                new EventJournal());
    }

    public EventCodec codec() {
        return codec;
    }

    public long allowedLatenessMs() {
        return watermarkManager.allowedLatenessMs();
    }

    public long bucketSizeMs() {
        return aggregationTable.bucketSizeMs();
    }

    public EventJournal journal() {
        return journal;
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
        MarketEvent parsedEvent = null;
        InvalidEventException parseError = null;
        try {
            parsedEvent = codec.decode(rawJson);
        } catch (InvalidEventException ex) {
            log.warn("[判定] INVALID eventId={} reason='{}' raw={}",
                    ex.getRawEventId(), ex.getMessage(), abbreviate(rawJson));
            parseError = ex;
        }
        final MarketEvent event = parsedEvent;

        // 全部结论（含 INVALID）在同一临界区登记事件日志，保证接收顺序与日志顺序一致。
        synchronized (lock) {
            IngestResult result = (parseError != null)
                    ? new IngestResult(IngestStatus.REJECTED_INVALID, parseError.getRawEventId(),
                            parseError.getMessage(), Long.MIN_VALUE, CheckpointManager.NO_OFFSET)
                    : classify(event, expectedOffsets);
            journal.append(new JournaledEvent(
                    journal.nextOrdinal(),
                    event == null ? null : event.source(),
                    result.eventId(),
                    rawJson,
                    Map.copyOf(expectedOffsets),
                    result.status(),
                    result.reason()));
            return result;
        }
    }

    /** 在持有锁的前提下完成 2~6 步判定，返回互斥结论之一。 */
    private IngestResult classify(MarketEvent event, Map<String, Long> expectedOffsets) {
            long committed = checkpointManager.committedOffsetOf(event.source());
            long watermark = watermarkManager.watermarkOf(event.source());

            // 重放/续处理：严格位点对齐校验
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

            // 3) 重复：同一 (source, eventId) 重发/重放天然幂等，先于位点回退判定。
            //    跨来源相同 eventId 是独立事件，不在此命中。
            EventKey dedupKey = EventKey.of(event);
            if (dedupTable.isDuplicate(dedupKey)) {
                String reason = "duplicate (source,eventId) already aggregated: "
                        + event.source() + "/" + event.eventId()
                        + "（该来源此前已收过同一事件）";
                log.info("[判定] DUPLICATE eventId={} source={} seq={} (不计入)",
                        event.eventId(), event.source(), event.sequence());
                return new IngestResult(IngestStatus.DUPLICATE,
                        event.eventId(), reason, watermark, committed);
            }

            // 4) 位点回退：新 eventId 却落在已提交位点，明确拒绝
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
            dedupTable.markSeen(dedupKey);
            aggregationTable.apply(event);
            long newWatermark = watermarkManager.advance(event.source(), event.eventTime());
            checkpointManager.commit(event.source(), event.sequence());

            log.info("[判定] ACCEPTED eventId={} source={} seq={} eventTime={} v={} symbol={} price={} qty={} venue={} -> watermark={} committed={}",
                    event.eventId(), event.source(), event.sequence(), event.eventTime(),
                    event.schemaVersion(), event.symbol(), event.price(), event.quantity(),
                    event.venue(), newWatermark, event.sequence());
            return new IngestResult(IngestStatus.ACCEPTED, event.eventId(),
                    "aggregated into bucket", newWatermark, event.sequence());
    }

    /** 原子持久化当前全量状态（先在锁内取一致快照，IO 在锁外执行）。 */
    public void snapshot() {
        final PipelineState state;
        synchronized (lock) {
            state = new PipelineState(
                    System.currentTimeMillis(),
                    watermarkManager.allowedLatenessMs(),
                    aggregationTable.bucketSizeMs(),
                    dedupTable.snapshotKeys(),
                    watermarkManager.snapshotWatermarks(),
                    checkpointManager.snapshotOffsets(),
                    aggregationTable.allViews());
        }
        snapshotRepository.save(state);
        log.info("[快照] 已持久化: seenKeys={} sources={} buckets={} savedAt={}",
                state.seenEventKeys().size(), state.committedOffsets().size(),
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
            dedupTable.restore(state.seenEventKeys());
            watermarkManager.restore(state.watermarks());
            checkpointManager.restore(state.committedOffsets());
            aggregationTable.restoreBuckets(state.buckets());
            log.info("[恢复] 已加载快照: seenKeys={} watermarks={} offsets={} buckets={} savedAt={}",
                    state.seenEventKeys().size(), state.watermarks(),
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

    private static String abbreviate(String raw) {
        if (raw == null) {
            return "null";
        }
        return raw.length() <= 160 ? raw : raw.substring(0, 160) + "...";
    }
}
