package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：多来源各自独立编号的幂等口径。
 *
 * <p>口径：幂等键为 (source, eventId)。
 * <ul>
 *   <li>不同来源 eventId 相同、甚至业务时间也相同：是各自独立事件，有几条算几条；</li>
 *   <li>同一来源重发同一 eventId：仍然只算一次，结论 DUPLICATE。</li>
 * </ul>
 * 两类结论必须一眼可区分（ACCEPTED vs DUPLICATE，reason 中带 source/eventId）。
 */
class MultiSourceIdempotencyTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSourceIdempotencyTest.class);
    private static final long T = 60_000L;

    @Test
    @DisplayName("不同来源相同编号、相同业务时间：各自独立计入，不被当同一条吞掉")
    void sameIdAcrossSourcesAreIndependentEvents() {
        var p = newPipeline();
        String a = TestSupport.event("1", "exchange-A", "AAA", "TRADE", T, 0, "100", 10);
        String b = TestSupport.event("1", "exchange-B", "AAA", "TRADE", T, 0, "101", 20);
        String c = TestSupport.event("1", "exchange-C", "AAA", "TRADE", T, 0, "102", 30);

        List<IngestResult> results = List.of(p.ingest(a), p.ingest(b), p.ingest(c));
        results.forEach(r -> log.info("跨源同号: eventId=1 source 结论={} reason='{}'",
                r.status(), r.reason()));

        assertTrue(results.stream().allMatch(r -> r.status() == IngestStatus.ACCEPTED),
                "三个来源的 1 号事件必须各自独立计入");
        long count = p.aggregates().stream().filter(v -> v.symbol().equals("AAA"))
                .mapToLong(v -> v.count()).sum();
        assertEquals(3, count, "有几条算几条，不能去重成一条");
        assertEquals(60, p.aggregates().stream().mapToLong(v -> v.totalQuantity()).sum());
        // 位点按来源各自独立：三源都提交到 0
        assertEquals(0, p.committedOffset("exchange-A"));
        assertEquals(0, p.committedOffset("exchange-B"));
        assertEquals(0, p.committedOffset("exchange-C"));
    }

    @Test
    @DisplayName("同一来源重发同编号：只计一次，结论 DUPLICATE，与跨源独立事件明确区分")
    void sameSourceResendIsDuplicate() {
        var p = newPipeline();
        String a1 = TestSupport.event("1", "exchange-A", "AAA", "TRADE", T, 0, "100", 10);
        String a1Again = TestSupport.event("1", "exchange-A", "AAA", "TRADE", T, 0, "100", 10);
        String b1 = TestSupport.event("1", "exchange-B", "AAA", "TRADE", T, 0, "101", 20);

        IngestResult firstA = p.ingest(a1);
        IngestResult secondA = p.ingest(a1Again);
        IngestResult independentB = p.ingest(b1);

        log.info("同源重发: 首次={} 重发={}（应 DUPLICATE）; 另一来源同号={}（应 ACCEPTED）",
                firstA.status(), secondA.status(), independentB.status());
        log.info("DUPLICATE 理由: '{}'", secondA.reason());

        assertEquals(IngestStatus.ACCEPTED, firstA.status());
        assertEquals(IngestStatus.DUPLICATE, secondA.status(),
                "同一来源重发同一条只能算一次");
        assertNotEquals(IngestStatus.DUPLICATE, independentB.status(),
                "别的来源的独立事件不能被当成重复吞掉");
        assertEquals(IngestStatus.ACCEPTED, independentB.status());
        assertTrue(secondA.reason().contains("exchange-A/1"),
                "理由必须指出是哪个来源的哪条已收过");

        long count = p.aggregates().stream().mapToLong(v -> v.count()).sum();
        assertEquals(2, count, "exchange-A#1 与 exchange-B#1 合计两条");
    }

    @Test
    @DisplayName("多源同号 + 并发重复投递：每个 (source,eventId) 恰好计入一次")
    void concurrentSameIdAcrossSourcesCountedOncePerSource() throws Exception {
        var p = newPipeline();
        int sources = 6;
        int perSource = 30;
        int duplicateThreads = 10;
        int duplicateRounds = 4;

        // 所有来源使用完全相同的 eventId 序列（0..29）与完全相同的业务时间，
        // 仅 source 不同 —— 最极端的跨源同号同刻。
        List<String> batch = new ArrayList<>();
        for (int s = 0; s < sources; s++) {
            for (int i = 0; i < perSource; i++) {
                batch.add(TestSupport.event(String.valueOf(i), "src-" + s, "AAA", "TRADE",
                        T + i * 10L, i, String.valueOf(100 + (i % 4)), 1));
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(sources, duplicateThreads));
        CountDownLatch firstPassDone = new CountDownLatch(sources);
        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();

        // 阶段一（真实分区语义）：每源一个线程，按 seq 顺序首次投递；多源之间完全并发。
        CountDownLatch firstStart = new CountDownLatch(1);
        for (int s = 0; s < sources; s++) {
            final int sourceIndex = s;
            pool.submit(() -> {
                try {
                    firstStart.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perSource; i++) {
                    String raw = TestSupport.event(String.valueOf(i), "src-" + sourceIndex,
                            "AAA", "TRADE", T + i * 10L, i,
                            String.valueOf(100 + (i % 4)), 1);
                    IngestResult res = p.ingest(raw);
                    results.add(res);
                    switch (res.status()) {
                        case ACCEPTED -> accepted.incrementAndGet();
                        case DUPLICATE -> duplicate.incrementAndGet();
                        default -> other.incrementAndGet();
                    }
                }
                firstPassDone.countDown();
            });
        }
        firstStart.countDown();
        assertTrue(firstPassDone.await(30, TimeUnit.SECONDS), "首轮投递必须完成");

        // 阶段二：全部事件已收过；10 线程 × 4 轮并发重复投递（顺序完全打乱）。
        // 同一 (source,eventId) 因去重判定先于位点回退判定，无论怎么乱序并发，
        // 结论恒为 DUPLICATE，绝不产生 OFFSET_REGRESSION。
        CountDownLatch dupStart = new CountDownLatch(1);
        for (int t = 0; t < duplicateThreads; t++) {
            pool.submit(() -> {
                try {
                    dupStart.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int r = 0; r < duplicateRounds; r++) {
                    for (String raw : batch) {
                        IngestResult res = p.ingest(raw);
                        results.add(res);
                        switch (res.status()) {
                            case ACCEPTED -> accepted.incrementAndGet();
                            case DUPLICATE -> duplicate.incrementAndGet();
                            default -> other.incrementAndGet();
                        }
                    }
                }
            });
        }
        dupStart.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int distinct = sources * perSource;
        log.info("多源同号并发: 总投递={} 独立事件 ACCEPTED={} 同源重发 DUPLICATE={} 其它拒绝={}",
                results.size(), accepted.get(), duplicate.get(), other.get());

        assertEquals(distinct, accepted.get(),
                "每个 (source,eventId) 恰好接受一次，跨源同号不得互相吞掉");
        assertEquals(results.size() - distinct, duplicate.get(),
                "其余全部是同源重发的幂等重复（含并发乱序重发）");
        assertEquals(0, other.get(), "并发重复重发不得触发位点回退/迟到等其它结论");
        for (int s = 0; s < sources; s++) {
            assertEquals(perSource - 1, p.committedOffset("src-" + s),
                    "位点按来源各自推进");
        }
        long totalCount = p.aggregates().stream().mapToLong(v -> v.count()).sum();
        assertEquals(distinct, totalCount);
    }
}
