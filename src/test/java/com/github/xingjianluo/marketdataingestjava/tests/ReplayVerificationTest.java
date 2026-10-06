package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.replay.RecordedIngestion;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayReport;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayVerificationResult;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayVerificationStatus;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayVerifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 基于历史事件的整体重放核对：把链路已收下的原始事件（摄取流水账）
 * 从干净空白状态完整重放一遍，逐条比对重放结论与首次处理结论，
 * 产出 总数/一致数/不一致数 + 不一致明细 的统计报告。
 */
class ReplayVerificationTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayVerificationTest.class);
    private static final long T = 60_000L;

    /** 构造一支覆盖多类结论的混合流水：跨源同号、同源重发、非法、迟到、位点回退。 */
    private IngestPipeline runMixedBatch() {
        var p = newPipeline();
        p.ingest(TestSupport.event("e1", "src-A", "AAA", "TRADE", T + 1000, 0, "100", 10));
        p.ingest(TestSupport.event("e2", "src-A", "AAA", "TRADE", T + 2000, 1, "101", 10));
        p.ingest(TestSupport.event("e1", "src-A", "AAA", "TRADE", T + 1000, 0, "100", 10)); // 同源重发 -> DUPLICATE
        p.ingest(TestSupport.event("e1", "src-B", "AAA", "TRADE", T + 1000, 0, "102", 20)); // 跨源同号同时刻 -> 独立计入
        p.ingest(TestSupport.event("e3", "src-A", "AAA", "TRADE", T + 3000, 2, "103", 10));
        p.ingest("{\"eventId\":\"bad\"}");                                                   // 非法结构
        p.ingest(TestSupport.event("eX", "src-A", "AAA", "TRADE", T + 2500, 1, "104", 10)); // 位点回退
        p.ingest(TestSupport.event("eL", "src-A", "AAA", "TRADE", T - 10000, 3, "105", 10)); // 超过迟到窗口
        return p;
    }

    @Test
    @DisplayName("干净状态整体重放：逐条结论与首次处理一致，统计全对得上")
    void fullReplayFromCleanStateMatchesFirstPass() {
        var p = runMixedBatch();
        List<RecordedIngestion> journal = p.journal();
        assertEquals(8, journal.size(), "每条投递（含被拒绝的）都必须入账");
        log.info("首次处理结论: {}", journal.stream()
                .map(j -> j.firstResult().status().name()).toList());

        ReplayReport report = ReplayVerifier.forPipeline(p).verify(journal);
        log.info("{}; 逐条一致={}", report.summary(), report.allMatched());

        assertEquals(8, report.totalReplayed());
        assertEquals(8, report.matched());
        assertEquals(0, report.mismatched());
        assertTrue(report.allMatched());
        assertTrue(report.mismatches().isEmpty());
    }

    @Test
    @DisplayName("重放从干净起点开始：不继承当前内存状态，首条事件仍按首次判定")
    void replayStartsFromBlankState() {
        var p = runMixedBatch();
        // 当前内存状态已推进（src-A 已提交位点 2、去重表非空）
        assertEquals(2, p.committedOffset("src-A"));

        // 若重放复用当前状态，首条 e1 会被判 DUPLICATE；
        // 干净起点下必须复现首次结论 ACCEPTED —— 全量一致即证明起点干净。
        ReplayReport report = ReplayVerifier.forPipeline(p).verify(p.journal());
        assertTrue(report.allMatched(), "重放必须从空白状态起步，任何状态泄漏都会导致首条结论漂移");
        assertEquals(IngestStatus.ACCEPTED, p.journal().get(0).firstResult().status());
        log.info("干净起点验证通过: {}", report.summary());
    }

    @Test
    @DisplayName("核对器能发现不一致：篡改首次结论后报告给出明细（哪条/期望/实际）")
    void verifierReportsMismatchDetails() {
        var p = runMixedBatch();
        List<RecordedIngestion> journal = new ArrayList<>(p.journal());

        // 篡改第 4 条（跨源同号事件，首次结论 ACCEPTED）为 DUPLICATE，
        // 模拟“当初记错了”的历史结论
        RecordedIngestion tampered = journal.get(3);
        IngestResult forged = new IngestResult(IngestStatus.DUPLICATE,
                tampered.firstResult().eventId(), "forged conclusion",
                tampered.firstResult().sourceWatermark(),
                tampered.firstResult().sourceCommittedOffset());
        journal.set(3, new RecordedIngestion(tampered.journalIndex(), tampered.rawJson(),
                tampered.expectedOffsets(), forged));

        ReplayReport report = ReplayVerifier.forPipeline(p).verify(journal);
        log.info("{}; 明细={}", report.summary(), report.mismatches());

        assertEquals(8, report.totalReplayed());
        assertEquals(7, report.matched());
        assertEquals(1, report.mismatched());
        var m = report.mismatches().get(0);
        assertEquals(3, m.journalIndex());
        assertEquals("e1", m.eventId());
        assertEquals(IngestStatus.DUPLICATE, m.expectedStatus(), "期望=被篡改的历史结论");
        assertEquals(IngestStatus.ACCEPTED, m.actualStatus(), "实际=干净重放的真实结论");
        assertTrue(m.toString().contains("e1"), "明细必须可读、能定位是哪一条");
    }

    @Test
    @DisplayName("续处理位置校验：比已提交位点靠前或靠后都明确拒绝，且拒绝本身也可重放复现")
    void resumeOffsetMustAlignExactly() {
        var p = newPipeline();
        for (int i = 0; i < 3; i++) {
            p.ingest(TestSupport.event("e" + i, "src-A", "AAA", "TRADE",
                    T + 1000L * i, i, "100", 10));
        }
        assertEquals(2, p.committedOffset("src-A"));

        // 靠前（回退）：从 0 号位点接 -> 拒绝
        IngestResult behind = p.resumeIngest(
                TestSupport.event("n1", "src-A", "AAA", "TRADE", T + 3000, 3, "100", 10),
                Map.of("src-A", 0L));
        log.info("续处理位点靠前 -> {} reason='{}'", behind.status(), behind.reason());
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION, behind.status());
        assertTrue(behind.reason().contains("regression refused"));

        // 靠后（跳跃）：从 5 号位点接 -> 拒绝（会漏计）
        IngestResult ahead = p.resumeIngest(
                TestSupport.event("n2", "src-A", "AAA", "TRADE", T + 6000, 6, "100", 10),
                Map.of("src-A", 5L));
        log.info("续处理位点靠后 -> {} reason='{}'", ahead.status(), ahead.reason());
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION, ahead.status());
        assertTrue(ahead.reason().contains("gap refused"));

        // 两次拒绝不得改变任何状态
        assertEquals(2, p.committedOffset("src-A"));
        assertEquals(3, p.aggregates().get(0).count());

        // 精确对齐：期望位点 == 已提交位点 -> 正常续处理
        IngestResult aligned = p.resumeIngest(
                TestSupport.event("n3", "src-A", "AAA", "TRADE", T + 3000, 3, "100", 10),
                Map.of("src-A", 2L));
        assertEquals(IngestStatus.ACCEPTED, aligned.status());
        assertEquals(3, p.committedOffset("src-A"));

        // 拒绝与接受的全部结论同样能被干净重放逐条复现
        ReplayReport report = ReplayVerifier.forPipeline(p).verify(p.journal());
        log.info("含位点拒绝的流水重放: {}", report.summary());
        assertTrue(report.allMatched());
        assertEquals(6, report.totalReplayed());
    }

    @Test
    @DisplayName("运行中的链路可直接发起核对：verifyReplay 从干净起点给出统计与明细")
    void pipelineVerifyReplayFromCleanStart() {
        var p = runMixedBatch();

        ReplayVerificationResult result = p.verifyReplay(null);
        log.info("verifyReplay(缺省位置) -> {} {}", result.status(), result.report().summary());
        assertEquals(ReplayVerificationStatus.VERIFIED, result.status());
        assertTrue(result.verifiedOk());
        assertEquals(8, result.report().totalReplayed());
        assertEquals(8, result.report().matched());
        assertEquals(0, result.report().mismatched());
        assertTrue(result.report().mismatches().isEmpty());

        // 显式给出干净起点（0，一条都还没处理过的位置）同样允许
        ReplayVerificationResult explicit = p.verifyReplay(0L);
        log.info("verifyReplay(干净起点 0) -> {} {}", explicit.status(), explicit.report().summary());
        assertEquals(ReplayVerificationStatus.VERIFIED, explicit.status());
        assertEquals(result.report(), explicit.report(), "同一批历史事件重放多少遍统计都必须一致");
    }

    @Test
    @DisplayName("核对发起的续处理位置：比干净起点靠前或靠后都明确拒绝，结论与正常核对可区分")
    void verifyReplayRejectsNonCleanStartPositions() {
        var p = runMixedBatch();

        // 靠前（回退）：从干净起点之前的位置发起 -> 拒绝
        ReplayVerificationResult behind = p.verifyReplay(-1L);
        log.info("核对续处理位置靠前 -> {} reason='{}'", behind.status(), behind.reason());
        assertEquals(ReplayVerificationStatus.REJECTED_OFFSET_REGRESSION, behind.status());
        assertTrue(behind.reason().contains("regression refused"));
        assertNull(behind.report(), "被拒绝的核对一条都不允许重放");

        // 靠后（跳跃）：从中间接上去会漏计 -> 拒绝
        ReplayVerificationResult ahead = p.verifyReplay(3L);
        log.info("核对续处理位置靠后 -> {} reason='{}'", ahead.status(), ahead.reason());
        assertEquals(ReplayVerificationStatus.REJECTED_OFFSET_REGRESSION, ahead.status());
        assertTrue(ahead.reason().contains("gap refused"));
        assertNull(ahead.report());

        // 拒绝结论与“核对通过”一眼可分（独立状态枚举，不与摄取结论混淆）
        assertNotEquals(ReplayVerificationStatus.VERIFIED, behind.status());
        assertNotEquals(ReplayVerificationStatus.VERIFIED, ahead.status());

        // 拒绝不触碰链路状态、不产生任何重放副作用
        assertEquals(2, p.committedOffset("src-A"));
        assertEquals(8, p.journal().size());
    }

    @Test
    @DisplayName("并发发起核对：多线程同时 verifyReplay 统计逐字段一致，且互不污染、不影响链路状态")
    void concurrentVerifyReplayConsistentAndNonPolluting() throws Exception {
        var p = runMixedBatch();
        long committedBefore = p.committedOffset("src-A");
        int journalBefore = p.journal().size();
        var aggregatesBefore = p.aggregates();

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ReplayVerificationResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return p.verifyReplay(null);
            }));
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        List<ReplayVerificationResult> results = new ArrayList<>();
        for (Future<ReplayVerificationResult> f : futures) {
            results.add(f.get());
        }
        ReplayReport first = results.get(0).report();
        for (ReplayVerificationResult r : results) {
            assertEquals(ReplayVerificationStatus.VERIFIED, r.status());
            assertEquals(8, r.report().totalReplayed());
            assertEquals(8, r.report().matched());
            assertTrue(r.report().allMatched());
            assertEquals(first, r.report(), "并发核对报告必须逐字段一致");
        }
        log.info("并发 {} 路核对全部一致: {}", threads, first.summary());

        // 核对过程不得反过来影响链路本身的状态
        assertEquals(committedBefore, p.committedOffset("src-A"));
        assertEquals(journalBefore, p.journal().size());
        assertEquals(aggregatesBefore, p.aggregates());
        log.info("核对后链路状态未变: committed={} journal={} buckets={}",
                committedBefore, journalBefore, aggregatesBefore.size());
    }
}
