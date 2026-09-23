package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.log.EventLog;
import com.github.xingjianluo.marketdataingestjava.log.LoggedRecord;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import com.github.xingjianluo.marketdataingestjava.protocol.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 全量重放：用空状态的新引擎按事件日志顺序重放全部原始事件。
 *
 * <p>保证：
 * <ul>
 *   <li>重放从空状态开始，配合幂等键/位点检查，不会重复累计也不会漏计；</li>
 *   <li>每条事件的重放结论与首次处理时记录的结论逐条比对（accepted/reason/窗口/水位线）；</li>
 *   <li>重放结束后的聚合结果（OHLC、分量成交量、笔数）与现网引擎当前结果逐项一致；</li>
 *   <li>重放引擎在独立状态目录上运行，绝不修改现网快照；位点回退保护同样生效。</li>
 * </ul>
 */
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final EventLog eventLog;
    private final Supplier<IngestEngine> freshEngineFactory;
    private final IngestEngine currentEngine;

    /**
     * @param eventLog           现网事件日志（读取原始记录与原始结论）
     * @param freshEngineFactory 空状态新引擎工厂（必须指向独立状态目录，避免恢复现网快照）
     * @param currentEngine      现网引擎，用于最终聚合结果对账
     */
    public ReplayService(EventLog eventLog, Supplier<IngestEngine> freshEngineFactory,
                         IngestEngine currentEngine) {
        this.eventLog = eventLog;
        this.freshEngineFactory = freshEngineFactory;
        this.currentEngine = currentEngine;
    }

    public ReplayReport replay() {
        var records = eventLog.readRecords();
        IngestEngine replay = freshEngineFactory.get();

        int accepted = 0;
        Map<String, Integer> rejectedByReason = new LinkedHashMap<>();
        int mismatches = 0;

        for (int i = 0; i < records.size(); i++) {
            LoggedRecord record = records.get(i);
            IngestOutcome outcome;
            try {
                outcome = replay.ingest(record.raw());
            } catch (RuntimeException e) {
                mismatches++;
                log.error("重放第 {} 条事件时抛出异常（首次处理并未抛出）: {}", i, e.toString());
                continue;
            }

            if (outcome.accepted()) {
                accepted++;
            } else {
                String reason = outcome.reason() == null
                        ? "UNKNOWN" : outcome.reason().name();
                rejectedByReason.merge(reason, 1, Integer::sum);
            }

            IngestOutcome original = record.outcome();
            String diff = compareVerdict(original, outcome);
            if (diff != null) {
                mismatches++;
                log.error("重放结论与首次处理不一致 index={} source={} seq={} diff={} | original={} | replay={}",
                        i, outcome.source(), outcome.seq(), diff, original, outcome);
            } else {
                log.info("重放判定一致 index={} source={} seq={} accepted={} reason={} window={} watermark={}",
                        i, outcome.source(), outcome.seq(), outcome.accepted(),
                        outcome.reason(), outcome.windowStart(), outcome.watermark());
            }
        }

        boolean aggregationsMatch = compareAggregations(currentEngine.aggregations(),
                replay.aggregations());
        boolean outcomesMatch = mismatches == 0 && aggregationsMatch;

        ReplayReport report = new ReplayReport(records.size(), accepted,
                rejectedByReason, outcomesMatch, mismatches, aggregationsMatch);
        log.info("全量重放完成 total={} accepted={} rejected={} verdictMismatches={} aggregationsMatch={} match={}",
                report.total(), report.accepted(), report.rejectedByReason(),
                report.verdictMismatches(), report.aggregationsMatch(), report.outcomesMatch());
        return report;
    }

    /** 逐字段比对判定结论，返回首个差异说明；一致返回 null。 */
    static String compareVerdict(IngestOutcome a, IngestOutcome b) {
        if (a == null || b == null) {
            return "结论缺失";
        }
        if (a.accepted() != b.accepted()) {
            return "accepted 不同: " + a.accepted() + " vs " + b.accepted();
        }
        RejectReason ra = a.reason();
        RejectReason rb = b.reason();
        if ((ra == null) != (rb == null) || ra != rb) {
            return "reason 不同: " + ra + " vs " + rb;
        }
        if (!eq(a.source(), b.source())) {
            return "source 不同: " + a.source() + " vs " + b.source();
        }
        if (a.seq() != b.seq()) {
            return "seq 不同: " + a.seq() + " vs " + b.seq();
        }
        if (!eq(a.eventId(), b.eventId())) {
            return "eventId 不同";
        }
        if (!eq(a.symbol(), b.symbol())) {
            return "symbol 不同";
        }
        if (!eq(a.windowStart(), b.windowStart())) {
            return "windowStart 不同: " + a.windowStart() + " vs " + b.windowStart();
        }
        if (a.watermark() != b.watermark()) {
            return "watermark 不同: " + a.watermark() + " vs " + b.watermark();
        }
        return null;
    }

    /** 对账两份聚合表：键集合一致，每个窗口的 OHLC/量/笔数全部一致。 */
    static boolean compareAggregations(Map<AggregationKey, WindowAggregation> expected,
                                       Map<AggregationKey, WindowAggregation> actual) {
        if (!expected.keySet().equals(actual.keySet())) {
            log.error("聚合窗口集合不同: onlyInCurrent={} onlyInReplay={}",
                    minus(expected.keySet(), actual.keySet()),
                    minus(actual.keySet(), expected.keySet()));
            return false;
        }
        for (AggregationKey key : expected.keySet()) {
            WindowAggregation e = expected.get(key);
            WindowAggregation a = actual.get(key);
            if (!sameAggregation(e, a)) {
                log.error("窗口聚合不一致 key={} current=[open={},high={},low={},close={},vol={},n={}] replay=[open={},high={},low={},close={},vol={},n={}]",
                        key, e.open(), e.high(), e.low(), e.close(), e.totalVolume(), e.tradeCount(),
                        a.open(), a.high(), a.low(), a.close(), a.totalVolume(), a.tradeCount());
                return false;
            }
        }
        return true;
    }

    private static boolean sameAggregation(WindowAggregation e, WindowAggregation a) {
        return cmp(e.open(), a.open()) == 0
                && cmp(e.high(), a.high()) == 0
                && cmp(e.low(), a.low()) == 0
                && cmp(e.close(), a.close()) == 0
                && e.totalVolume() == a.totalVolume()
                && e.tradeCount() == a.tradeCount()
                && e.buyVolume() == a.buyVolume()
                && e.sellVolume() == a.sellVolume()
                && e.unknownVolume() == a.unknownVolume();
    }

    private static int cmp(BigDecimal a, BigDecimal b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null || b == null) {
            return a == null ? -1 : 1;
        }
        return a.compareTo(b);
    }

    private static boolean eq(Object a, Object b) {
        return a == null ? b == null : a.equals(b);
    }

    private static <T> java.util.Set<T> minus(java.util.Set<T> a, java.util.Set<T> b) {
        java.util.Set<T> diff = new java.util.LinkedHashSet<>(a);
        diff.removeAll(b);
        return diff;
    }

    /**
     * @param total             重放事件总数
     * @param accepted          计入聚合的数量
     * @param rejectedByReason  各拒绝原因的数量
     * @param outcomesMatch     结论与聚合是否全部一致
     * @param verdictMismatches 逐条结论不一致的数量
     * @param aggregationsMatch 最终聚合表是否对账一致
     */
    public record ReplayReport(int total, int accepted,
                               Map<String, Integer> rejectedByReason,
                               boolean outcomesMatch, int verdictMismatches,
                               boolean aggregationsMatch) {}
}
