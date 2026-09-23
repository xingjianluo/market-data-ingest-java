package com.github.xingjianluo.marketdataingestjava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationBook;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedup.Deduplicator;
import com.github.xingjianluo.marketdataingestjava.log.EventLog;
import com.github.xingjianluo.marketdataingestjava.offset.CursorTracker;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import com.github.xingjianluo.marketdataingestjava.protocol.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.protocol.RejectReason;
import com.github.xingjianluo.marketdataingestjava.state.AggregationEntry;
import com.github.xingjianluo.marketdataingestjava.state.EngineSnapshot;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotStore;
import com.github.xingjianluo.marketdataingestjava.window.LatenessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 摄取核心：原始 JSON 信封 → 解码/位点/去重/迟到/聚合 → 事件日志与状态快照。
 *
 * <p>单把全局锁保证：多源可并发提交，但每条事件的“判定 + 状态变更 + 持久化”原子执行，
 * 因此聚合结果与到达顺序无关，只取决于事件集合本身（乱序并入结论一致、可复现）。
 *
 * <p>处理顺序（每一步拒绝都带可区分原因）：
 * <ol>
 *   <li>结构解码：非法事件以 {@link RejectReason#INVALID_EVENT} 拒绝，仍推进位点；</li>
 *   <li>位点检查：回退 → {@link RejectReason#OFFSET_BACKWARD}，跳跃 → {@link RejectReason#OFFSET_GAP}；</li>
 *   <li>最后位点重投：返回该位点上缓存的原始判定（重复重投得到 DUPLICATE，结论可复现）；</li>
 *   <li>幂等去重：幂等键已见 → {@link RejectReason#DUPLICATE}，推进位点但不累计；</li>
 *   <li>迟到判定：超期窗口 → {@link RejectReason#LATE}，推进位点但不累计；</li>
 *   <li>接受：并入聚合、推进水位线、推进位点。</li>
 * </ol>
 * 每次处理结束都追加事件日志并原子保存快照，保证中断后可从最后位点断点续处理。
 */
public class IngestEngine {

    private static final Logger log = LoggerFactory.getLogger(IngestEngine.class);

    private final LatenessPolicy policy;
    private final EventCodec codec;
    private final CursorTracker cursors;
    private final Deduplicator deduplicator;
    private final AggregationBook book;
    private final SnapshotStore snapshotStore;
    private final EventLog eventLog;
    private final boolean persistLog;

    private final Object lock = new Object();
    private long watermark = Long.MIN_VALUE;

    /** 生产构造：在 stateDir 下使用 snapshot.json 与 events.jsonl，启动时自动恢复。 */
    public IngestEngine(LatenessPolicy policy, Path stateDir) {
        this(policy,
                new EventCodec(new ObjectMapper()),
                new CursorTracker(), new Deduplicator(), new AggregationBook(),
                new SnapshotStore(stateDir.resolve("snapshot.json"), new ObjectMapper()),
                new EventLog(stateDir.resolve("events.jsonl")),
                true);
        restoreIfPresent();
    }

    /** 完整注入构造（测试/重放使用）。persistLog=false 时为全量重放引擎，不重复追加日志。 */
    public IngestEngine(LatenessPolicy policy, EventCodec codec, CursorTracker cursors,
                        Deduplicator deduplicator, AggregationBook book,
                        SnapshotStore snapshotStore, EventLog eventLog, boolean persistLog) {
        this.policy = policy;
        this.codec = codec;
        this.cursors = cursors;
        this.deduplicator = deduplicator;
        this.book = book;
        this.snapshotStore = snapshotStore;
        this.eventLog = eventLog;
        this.persistLog = persistLog;
    }

    /** 从快照恢复（生产构造时自动调用）。 */
    public final void restoreIfPresent() {
        synchronized (lock) {
            snapshotStore.load().ifPresent(s -> {
                cursors.restore(s.cursors());
                watermark = s.watermark();
                deduplicator.restore(s.dedupKeys(), s.verdictCache());
                Map<AggregationKey, WindowAggregation> data = new java.util.LinkedHashMap<>();
                for (AggregationEntry entry : s.aggregations()) {
                    AggregationKey key = new AggregationKey(entry.symbol(), entry.windowStart());
                    data.put(key, WindowAggregation.fromState(entry.state()));
                }
                book.restore(data);
                log.info("引擎状态已从快照恢复 sources={} watermark={} 聚合窗口={}",
                        s.cursors(), s.watermark(), s.aggregations().size());
            });
        }
    }

    /** 处理一条原始 JSON 信封，永远返回带明确依据的结论，不抛业务异常。 */
    public IngestOutcome ingest(String rawJson) {
        synchronized (lock) {
            IngestOutcome outcome = doIngest(rawJson);
            eventLog.append(rawJson, outcome);
            persist();
            log.info("摄取判定 accepted={} reason={} source={} seq={} eventId={} symbol={} window={} watermark={} detail={}",
                    outcome.accepted(), outcome.reason(), outcome.source(), outcome.seq(),
                    outcome.eventId(), outcome.symbol(), outcome.windowStart(),
                    outcome.watermark(), outcome.detail());
            return outcome;
        }
    }

    private IngestOutcome doIngest(String rawJson) {
        EventCodec.ParseResult parsed;
        try {
            parsed = codec.parse(rawJson);
        } catch (RuntimeException e) {
            parsed = new EventCodec.Unparseable(null, null, null,
                    "解码发生未预期异常: " + e.getClass().getSimpleName() + ":" + e.getMessage());
        }

        if (parsed instanceof EventCodec.Unparseable bad) {
            // 结构非法：没有可信位点信息时无法推进任何来源游标，仅记录拒绝
            String src = bad.source();
            Long seq = bad.seq();
            if (src == null || seq == null) {
                return IngestOutcome.rejected(RejectReason.INVALID_EVENT,
                        src == null ? "unknown" : src, seq == null ? -1L : seq,
                        bad.eventId(), null, null, watermark,
                        "非法事件且缺少可信 source/seq，位点无法推进：" + bad.detail());
            }
            CursorTracker.CursorCheck check = cursors.check(src, seq);
            if (check.status() == CursorTracker.Status.BACKWARD) {
                return rejectOffsetBackward(src, seq, bad.eventId(), check);
            }
            if (check.status() == CursorTracker.Status.GAP) {
                return rejectGap(src, seq, bad.eventId(), check);
            }
            if (check.status() == CursorTracker.Status.RETRY_LAST) {
                return deduplicator.cachedVerdict(src, seq)
                        .orElseGet(() -> IngestOutcome.rejected(RejectReason.INVALID_EVENT,
                                src, seq, bad.eventId(), null, null, watermark, bad.detail()));
            }
            cursors.advance(src, seq);
            IngestOutcome outcome = IngestOutcome.rejected(RejectReason.INVALID_EVENT,
                    src, seq, bad.eventId(), null, null, watermark, bad.detail());
            deduplicator.cacheVerdict(src, seq, outcome);
            return outcome;
        }

        EventCodec.Parsed ok = (EventCodec.Parsed) parsed;
        MarketEvent event = ok.event();
        String source = event.source();
        long seq = event.seq();
        CursorTracker.CursorCheck check = cursors.check(source, seq);

        return switch (check.status()) {
            case BACKWARD -> rejectOffsetBackward(source, seq, event.eventId(), check);
            case GAP -> rejectGap(source, seq, event.eventId(), check);
            case RETRY_LAST -> deduplicator.cachedVerdict(source, seq)
                    .orElseGet(() -> IngestOutcome.rejected(RejectReason.INVALID_EVENT,
                            source, seq, event.eventId(), null, null, watermark,
                            "最后位点重投但缺少缓存判定（状态不一致，保守拒绝）"));
            case FIRST, ADVANCE -> processFresh(event);
        };
    }

    private IngestOutcome processFresh(MarketEvent event) {
        String source = event.source();
        long seq = event.seq();
        long windowStart = policy.windowStart(event.eventTime());

        if (!deduplicator.firstSeen(source, event.eventId())) {
            cursors.advance(source, seq);
            IngestOutcome dup = IngestOutcome.rejected(RejectReason.DUPLICATE, source, seq,
                    event.eventId(), event.symbol(), windowStart, watermark,
                    "幂等键 (source=" + source + ",eventId=" + event.eventId()
                            + ") 已处理过，不重复计入，位点推进至 " + seq);
            deduplicator.cacheVerdict(source, seq, dup);
            return dup;
        }

        if (policy.isLate(event.eventTime(), watermark)) {
            cursors.advance(source, seq);
            IngestOutcome late = IngestOutcome.rejected(RejectReason.LATE, source, seq,
                    event.eventId(), event.symbol(), windowStart, watermark,
                    "事件时间 " + event.eventTime() + " 早于拒绝阈值 "
                            + (watermark - policy.allowedLatenessMillis())
                            + "（watermark=" + watermark + ", allowedLateness="
                            + policy.allowedLatenessMillis() + "），窗口 " + windowStart
                            + " 已封存");
            deduplicator.cacheVerdict(source, seq, late);
            return late;
        }

        book.apply(event, windowStart);
        if (event.eventTime() > watermark) {
            watermark = event.eventTime();
        }
        cursors.advance(source, seq);
        IngestOutcome accepted = IngestOutcome.accepted(source, seq, event.eventId(),
                event.symbol(), windowStart, watermark,
                "事件有效：版本=" + event.schemaVersion() + " 并入窗口 " + windowStart
                        + "，水位线推进/保持为 " + watermark);
        deduplicator.cacheVerdict(source, seq, accepted);
        return accepted;
    }

    private IngestOutcome rejectOffsetBackward(String source, long seq, String eventId,
                                               CursorTracker.CursorCheck check) {
        return IngestOutcome.rejected(RejectReason.OFFSET_BACKWARD, source, seq, eventId,
                null, null, watermark,
                "位点回退被拒绝：seq=" + seq + " 早于已提交位点 " + check.lastCommitted()
                        + "，期望位点 " + check.expected()
                        + "。只能从已提交位点+1 断点续处理，或在空状态下全量重放");
    }

    private IngestOutcome rejectGap(String source, long seq, String eventId,
                                    CursorTracker.CursorCheck check) {
        return IngestOutcome.rejected(RejectReason.OFFSET_GAP, source, seq, eventId,
                null, null, watermark,
                "位点跳跃被拒绝：seq=" + seq + " 大于期望位点 " + check.expected()
                        + "，存在漏投；请补齐缺口或在空状态下全量重放");
    }

    private void persist() {
        snapshotStore.save(snapshot());
    }

    public long watermark() {
        synchronized (lock) {
            return watermark;
        }
    }

    public Map<String, Long> cursors() {
        return cursors.positions();
    }

    public Map<AggregationKey, WindowAggregation> aggregations() {
        return book.snapshot();
    }

    public EventLog eventLog() {
        return eventLog;
    }

    public EngineSnapshot snapshot() {
        synchronized (lock) {
            List<AggregationEntry> entries = new ArrayList<>();
            book.snapshot().forEach((key, agg) ->
                    entries.add(new AggregationEntry(key.symbol(), key.windowStart(), agg.toState())));
            return new EngineSnapshot(cursors.positions(), watermark,
                    deduplicator.dedupKeys(), deduplicator.verdictCache(), entries);
        }
    }
}
