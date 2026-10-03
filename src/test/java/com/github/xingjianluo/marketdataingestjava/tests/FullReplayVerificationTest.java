package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayMismatch;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayRefusedException;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayReport;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：基于历史原始事件的整体重放核对。
 *
 * <p>从干净空白状态把日志中的原始事件完整重放，逐条与首次处理结论比对，
 * 输出总数/一致数/不一致数与明细；并覆盖干净起点的位点拒绝口径。
 */
class FullReplayVerificationTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(FullReplayVerificationTest.class);
    private static final long T = 60_000L;

    @Test
    @DisplayName("多源同号 + 各类结论混合：干净整体重放逐条一致，聚合逐字段一致")
    void fullReplayFromScratchMatchesEveryOriginalConclusion() {
        var p = newPipeline();
        // 三个来源，eventId/时间刻意重叠，且混入 DUPLICATE / LATE / INVALID / OFFSET_REGRESSION
        p.ingest(TestSupport.event("1", "exchange-A", "AAA", "TRADE", T, 0, "100", 10));
        p.ingest(TestSupport.event("1", "exchange-B", "AAA", "TRADE", T, 0, "101", 20));
        p.ingest(TestSupport.event("1", "exchange-A", "AAA", "TRADE", T, 0, "100", 10)); // 同源重发 -> DUPLICATE
        p.ingest(TestSupport.event("2", "exchange-A", "AAA", "TRADE", T + 10_000, 1, "110", 5));
        p.ingest(TestSupport.event("3", "exchange-A", "AAA", "TRADE", T, 2, "99", 1));    // 超期 -> LATE
        p.ingest(TestSupport.raw("{\"eventId\":\"bad\",\"source\":\"exchange-A\"}"));      // INVALID
        p.ingest(TestSupport.event("old", "exchange-B", "AAA", "TRADE", T + 1_000, 0, "98", 2)); // 回退 -> REGRESSION
        p.ingest(TestSupport.event("2", "exchange-B", "AAA", "TRADE", T + 1_000, 1, "105", 7));

        int journalSize = p.journal().size();
        log.info("首次处理完成，日志条数={}", journalSize);
        assertEquals(8, journalSize);

        ReplayService replay = new ReplayService(p);
        ReplayReport report = replay.replayFromScratch();

        log.info("[核对统计] 重放={} 一致={} 不一致={} 聚合一致={}",
                report.totalReplayed(), report.matched(), report.mismatched(),
                report.aggregatesMatch());
        assertEquals(journalSize, report.totalReplayed());
        assertEquals(journalSize, report.matched());
        assertEquals(0, report.mismatched());
        assertTrue(report.mismatches().isEmpty());
        assertTrue(report.aggregatesMatch());
        assertTrue(report.allMatched());

        // 重放绝不触碰线上状态：线上计数不变
        long liveCount = p.aggregates().stream().mapToLong(v -> v.count()).sum();
        assertEquals(4, liveCount, "被接受事件：A#1,B#1,A#2,B#2 共 4 条");
    }

    @Test
    @DisplayName("重放使用独立的干净状态：线上状态不被重放改动，且重放后可再次重放结果相同")
    void replayDoesNotMutateLiveStateAndIsReusable() {
        var p = newPipeline();
        for (int i = 0; i < 5; i++) {
            p.ingest(TestSupport.event(String.valueOf(i), "src", "AAA", "TRADE",
                    T + 1_000L * i, i, "100", 1));
        }
        Object liveAgg = p.aggregates();
        var replay = new ReplayService(p);

        ReplayReport r1 = replay.replayFromScratch();
        ReplayReport r2 = replay.replayFromScratch();
        assertTrue(r1.allMatched());
        assertEquals(r1.totalReplayed(), r2.totalReplayed());
        assertEquals(r1.matched(), r2.matched());
        assertEquals(liveAgg, p.aggregates(), "两次重放后线上聚合必须原样不变");
        log.info("两次干净重放结论一致，且线上状态未被触碰");
    }

    @Test
    @DisplayName("干净重放时给更靠前(0)或更靠后(5)的续处理位置：明确拒绝，绝不从中间接上")
    void nonCleanStartOffsetsAreRefused() {
        var p = newPipeline();
        p.ingest(TestSupport.event("0", "src", "AAA", "TRADE", T, 0, "100", 1));
        var replay = new ReplayService(p);

        ReplayRefusedException backwards = assertThrows(ReplayRefusedException.class,
                () -> replay.replayFromScratch(Map.of("src", 0L)));
        ReplayRefusedException forwards = assertThrows(ReplayRefusedException.class,
                () -> replay.replayFromScratch(Map.of("src", 5L)));
        log.info("更靠前位置拒绝理由: {}", backwards.getMessage());
        log.info("更靠后位置拒绝理由: {}", forwards.getMessage());

        // -1（干净起点）显式给出也必须允许
        ReplayReport ok = replay.replayFromScratch(Map.of("src", -1L));
        assertEquals(1, ok.totalReplayed());
        assertTrue(ok.allMatched());
    }

    @Test
    @DisplayName("首次处理含续处理位点校验的历史，干净重放也逐条复现拒绝/接受结论")
    void replayReproducesResumeOffsetDecisions() {
        var p = newPipeline();
        // 首次处理：以续处理严格位点模式接收 seq0、seq1
        assertEquals(IngestStatus.ACCEPTED,
                p.resumeIngest(TestSupport.event("0", "src", "AAA", "TRADE", T, 0, "100", 1),
                        Map.of("src", -1L)).status());
        assertEquals(IngestStatus.ACCEPTED,
                p.resumeIngest(TestSupport.event("1", "src", "AAA", "TRADE", T + 1_000, 1, "100", 1),
                        Map.of("src", 0L)).status());
        // 首次处理时给错位点 -> OFFSET_REGRESSION（历史结论必须保留并可复现）
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION,
                p.resumeIngest(TestSupport.event("9", "src", "AAA", "TRADE", T + 2_000, 9, "100", 1),
                        Map.of("src", 7L)).status());

        ReplayReport report = new ReplayService(p).replayFromScratch();
        log.info("带位点历史重放: total={} matched={} mismatched={}",
                report.totalReplayed(), report.matched(), report.mismatched());
        assertTrue(report.allMatched(), "干净重放必须复现首次的 ACCEPTED/ACCEPTED/REGRESSION 三条结论");
    }

    @Test
    @DisplayName("不一致时给出具体明细：哪一条、期望什么、实际什么（真实验证重放核对的检出能力）")
    void mismatchReportContainsOrdinalExpectedAndActual() {
        // 构造一条“首次结论与干净重放必然不同”的历史日志：
        // 线上内存状态已经把 src#0 计入（committed=0），而日志中该事件首次结论被记为
        // DUPLICATE（等价于“历史结论记录与状态不符”的污染场景）。
        // 从干净状态重放同一条原始事件时，空状态必然给出 ACCEPTED，与记录的 DUPLICATE 冲突，
        // 核对必须把它逐条检出并给出完整明细。
        var p = newPipeline();
        p.ingest(TestSupport.event("0", "src", "AAA", "TRADE", T, 0, "100", 1));
        p.journal().clear(); // 清掉真实历史，只保留内存状态（已计入 src#0）
        p.journal().append(new com.github.xingjianluo.marketdataingestjava.replay.JournaledEvent(
                0, "src", "0",
                TestSupport.event("0", "src", "AAA", "TRADE", T, 0, "100", 1),
                java.util.Map.of(),
                IngestStatus.DUPLICATE,
                "simulated historical conclusion: DUPLICATE"));

        ReplayReport report = new ReplayService(p).replayFromScratch();
        log.info("[检出核对] total={} matched={} mismatched={} aggregatesMatch={}",
                report.totalReplayed(), report.matched(), report.mismatched(),
                report.aggregatesMatch());
        assertEquals(1, report.totalReplayed());
        assertEquals(0, report.matched());
        assertEquals(1, report.mismatched());
        assertFalse(report.allMatched());

        ReplayMismatch sample = report.mismatches().get(0);
        log.info("不一致明细: #{} source={} eventId={} expected={} actual={} expectedReason='{}' actualReason='{}'",
                sample.ordinal(), sample.source(), sample.eventId(),
                sample.expectedStatus(), sample.actualStatus(),
                sample.expectedReason(), sample.actualReason());
        assertEquals(0, sample.ordinal());
        assertEquals("src", sample.source());
        assertEquals("0", sample.eventId());
        assertEquals(IngestStatus.DUPLICATE, sample.expectedStatus());
        assertEquals(IngestStatus.ACCEPTED, sample.actualStatus());
        assertFalse(sample.expectedReason().isBlank());
        assertFalse(sample.actualReason().isBlank());
    }
}
