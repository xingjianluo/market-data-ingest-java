package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import com.github.xingjianluo.marketdataingestjava.protocol.RejectReason;
import com.github.xingjianluo.marketdataingestjava.window.LatenessPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 去重、迟到窗口、位点保护与原因区分的单源核心测试。 */
class IngestEngineCoreTest {

    private static final Logger log = LoggerFactory.getLogger(IngestEngineCoreTest.class);

    // 窗口 10s，允许迟到 2s，时间以毫秒给出
    private static final long W = 10_000;
    private static final long L = 2_000;

    private IngestEngine newEngine(Path dir) {
        return new IngestEngine(new LatenessPolicy(W, L), dir);
    }

    private IngestOutcome run(IngestEngine engine, String raw) {
        IngestOutcome o = engine.ingest(raw);
        log.info("位点/水位 source={} seq={} 判定 accepted={} reason={} window={} watermark={} 依据={}",
                o.source(), o.seq(), o.accepted(), o.reason(), o.windowStart(), o.watermark(),
                o.detail());
        return o;
    }

    @Test
    void duplicateEventIdCountedOnceAndRetryLastReturnsDuplicate(@TempDir Path dir) {
        IngestEngine engine = newEngine(dir);
        String first = Events.v1("A", 0, "A-0", "BTCUSDT", 1_000, "100", 1);
        String sameIdNewSeq = Events.v2("A", 1, "A-0", "BTCUSDT", 2_000, "200", 5, "BUY");

        assertTrue(run(engine, first).accepted());
        IngestOutcome dup = run(engine, sameIdNewSeq);
        assertFalse(dup.accepted());
        assertEquals(RejectReason.DUPLICATE, dup.reason());

        // 最后位点重投（at-least-once 重试）：仍判定为 DUPLICATE，且不重复累计
        IngestOutcome retry = run(engine, sameIdNewSeq);
        assertEquals(RejectReason.DUPLICATE, retry.reason());

        WindowAggregation agg = engine.aggregations()
                .get(new AggregationKey("BTCUSDT", 0L));
        assertNotNull(agg);
        assertEquals(1, agg.tradeCount(), "重复事件不得重复计入");
        assertEquals(1, agg.totalVolume());
        log.info("[去重] 窗口聚合保持单次计数 count={} volume={} close={}",
                agg.tradeCount(), agg.totalVolume(), agg.close());
    }

    @Test
    void withinLatenessEventMergesIntoExistingWindow(@TempDir Path dir) {
        IngestEngine engine = newEngine(dir);
        // seq0: t=5000（水位线抬到 5000）；seq1: t=3000 乱序，仍在水位线 5000-2000=3000 边界内
        assertTrue(run(engine, Events.v1("A", 0, "A-0", "BTCUSDT", 5_000, "110", 2)).accepted());
        IngestOutcome lateOk = run(engine, Events.v1("A", 1, "A-1", "BTCUSDT", 3_000, "90", 3));
        assertTrue(lateOk.accepted(), "未超过允许迟到窗口的乱序事件必须并入");
        assertEquals(0L, lateOk.windowStart());

        WindowAggregation agg = engine.aggregations().get(new AggregationKey("BTCUSDT", 0L));
        // open 以更早事件时间 t=3000 的价格 90 为准（与到达顺序无关），close 以 t=5000 的 110 为准
        assertEquals("90", agg.open().toPlainString());
        assertEquals("110", agg.close().toPlainString());
        assertEquals(5, agg.totalVolume());
        assertEquals(2, agg.tradeCount());
        log.info("[乱序并入] open={}(t3000) close={}(t5000) high={} low={} vol={}",
                agg.open(), agg.close(), agg.high(), agg.low(), agg.totalVolume());
    }

    @Test
    void beyondLatenessRejectedAndDistinguishable(@TempDir Path dir) {
        IngestEngine engine = newEngine(dir);
        assertTrue(run(engine, Events.v1("A", 0, "A-0", "BTCUSDT", 10_000, "100", 1)).accepted());
        // t=7999 < 水位线10000 - 允许迟到2000 = 8000，超期
        IngestOutcome tooLate = run(engine, Events.v1("A", 1, "A-1", "BTCUSDT", 7_999, "99", 1));
        assertFalse(tooLate.accepted());
        assertEquals(RejectReason.LATE, tooLate.reason());
        // 窗口 0 已封存，不得并入；t=10000 落入窗口 10000
        assertEquals(0L, tooLate.windowStart());
        assertTrue(tooLate.detail().contains("已封存"));
        log.info("[超期拒绝] 原因={} 阈值依据={}", tooLate.reason(), tooLate.detail());
    }

    @Test
    void offsetBackwardAndGapAreRejectedNotContinued(@TempDir Path dir) {
        IngestEngine engine = newEngine(dir);
        assertTrue(run(engine, Events.v1("A", 0, "A-0", "X", 1_000, "1", 1)).accepted());
        assertTrue(run(engine, Events.v1("A", 1, "A-1", "X", 2_000, "1", 1)).accepted());

        // 回退到 seq=0（已提交位点为 1）：明确拒绝，位点不移动，绝不从中间继续
        IngestOutcome backward = run(engine, Events.v1("A", 0, "TAMPERED", "X", 1_000, "9", 9));
        assertEquals(RejectReason.OFFSET_BACKWARD, backward.reason());
        // 跳跃 seq=5
        IngestOutcome gap = run(engine, Events.v1("A", 5, "A-5", "X", 3_000, "1", 1));
        assertEquals(RejectReason.OFFSET_GAP, gap.reason());

        // 期望位点仍是 2，回退后没有“从中间继续”
        assertEquals(1L, engine.cursors().get("A"));
        log.info("[位点保护] 回退判定={} 跳跃判定={} 当前位点={}",
                backward.reason(), gap.reason(), engine.cursors().get("A"));
    }

    @Test
    void invalidEventRejectedButCursorAdvancesAndReasonsDistinct(@TempDir Path dir) {
        IngestEngine engine = newEngine(dir);
        String badPrice = Events.invalid("A", 0, "A-0",
                "{\"symbol\":\"X\",\"eventTime\":1000,\"price\":\"oops\",\"size\":1}");
        IngestOutcome invalid = run(engine, badPrice);
        assertEquals(RejectReason.INVALID_EVENT, invalid.reason());

        // 非法事件也推进位点，来源不被卡死
        assertTrue(run(engine, Events.v1("A", 1, "A-1", "X", 10_000, "2", 1)).accepted());
        assertEquals(1L, engine.cursors().get("A"));

        // seq2：水位线 10000、阈值 8000，t=5000 超期
        IngestOutcome late = run(engine, Events.v1("A", 2, "A-2", "X", 5_000, "1", 1));
        assertEquals(RejectReason.LATE, late.reason());

        // seq3：复用 eventId=A-1，重复
        IngestOutcome dup = run(engine, Events.v1("A", 3, "A-1", "X", 11_000, "1", 1));
        assertEquals(RejectReason.DUPLICATE, dup.reason());

        // seq9：位点跳跃
        IngestOutcome gap = run(engine, Events.v1("A", 9, "A-9", "X", 12_000, "1", 1));
        assertEquals(RejectReason.OFFSET_GAP, gap.reason());

        // 已提交位点停在 3，回退 seq=1 被拒绝
        IngestOutcome backward = run(engine, Events.v1("A", 1, "A-1", "X", 10_000, "2", 1));
        assertEquals(RejectReason.OFFSET_BACKWARD, backward.reason());

        log.info("[原因区分] INVALID_EVENT / LATE / DUPLICATE / OFFSET_GAP / OFFSET_BACKWARD 互斥，当前位点={}",
                engine.cursors().get("A"));
    }
}
