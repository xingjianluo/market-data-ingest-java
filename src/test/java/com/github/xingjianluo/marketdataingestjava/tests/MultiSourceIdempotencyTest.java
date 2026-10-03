package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多来源独立编号下的幂等口径：
 * 不同来源的相同 eventId（哪怕业务时间完全相同）是各自独立的业务事件，分别计入；
 * 同一来源重发同一 eventId 仍只算一次。两种结论在 reason 中可一眼区分。
 */
class MultiSourceIdempotencyTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(MultiSourceIdempotencyTest.class);
    private static final long T = 60_000L;

    @Test
    @DisplayName("多源同号同时刻：各自独立计入；同源重发判 DUPLICATE，两类结论可区分")
    void sameIdAcrossSourcesCountedIndependently() {
        var p = newPipeline();

        // 两个来源各自从 1 号开始编号，eventId 与业务时间完全相同
        IngestResult a1 = p.ingest(TestSupport.event("1", "src-A", "AAA", "TRADE", T, 1, "100", 10));
        IngestResult b1 = p.ingest(TestSupport.event("1", "src-B", "AAA", "TRADE", T, 1, "101", 20));
        IngestResult c1 = p.ingest(TestSupport.event("1", "src-C", "AAA", "TRADE", T, 1, "102", 30));

        log.info("src-A#1 -> {} reason='{}'", a1.status(), a1.reason());
        log.info("src-B#1 -> {} reason='{}'", b1.status(), b1.reason());
        log.info("src-C#1 -> {} reason='{}'", c1.status(), c1.reason());

        assertEquals(IngestStatus.ACCEPTED, a1.status());
        assertEquals(IngestStatus.ACCEPTED, b1.status(), "别的来源的同号事件是独立业务，必须计入");
        assertEquals(IngestStatus.ACCEPTED, c1.status());
        // 结论可一眼区分：后两条显式标注“跨源独立事件”
        assertTrue(b1.reason().contains("independent event"), "跨源独立事件必须在结论中标注");
        assertTrue(b1.reason().contains("src-A"));
        assertTrue(c1.reason().contains("independent event"));

        // 三条独立业务全部计入同一桶
        AggregateView view = p.aggregates().get(0);
        assertEquals(3, view.count());
        assertEquals(60, view.totalQuantity());

        // 同源重发：每个来源各自重发自己的 1 号，都只能算一次
        IngestResult aDup = p.ingest(TestSupport.event("1", "src-A", "AAA", "TRADE", T, 1, "100", 10));
        IngestResult bDup = p.ingest(TestSupport.event("1", "src-B", "AAA", "TRADE", T, 1, "101", 20));
        log.info("src-A 重发#1 -> {} reason='{}'", aDup.status(), aDup.reason());
        log.info("src-B 重发#1 -> {} reason='{}'", bDup.status(), bDup.reason());

        assertEquals(IngestStatus.DUPLICATE, aDup.status());
        assertEquals(IngestStatus.DUPLICATE, bDup.status());
        assertTrue(aDup.reason().contains("already aggregated from source='src-A'"),
                "重复结论必须指明是哪个来源的重发");
        assertTrue(bDup.reason().contains("already aggregated from source='src-B'"));

        // 重发未被计入，聚合不变
        assertEquals(3, p.aggregates().get(0).count());
        assertEquals(60, p.aggregates().get(0).totalQuantity());
    }

    @Test
    @DisplayName("多源各自从 1 号编号：位点/水位按源独立推进，互不干扰")
    void perSourceOffsetsAreIndependent() {
        var p = newPipeline();
        // src-A 连续 3 条（seq 1,2,3），src-B 只发 1 条（seq 1），编号口径各自独立
        for (int i = 1; i <= 3; i++) {
            assertEquals(IngestStatus.ACCEPTED, p.ingest(TestSupport.event(
                    "A-" + i, "src-A", "AAA", "TRADE", T + i * 1000L, i, "100", 1)).status());
        }
        // src-B 的 seq=1 不因 src-A 已推进到 3 而被判回退
        IngestResult b1 = p.ingest(TestSupport.event("B-1", "src-B", "AAA", "TRADE", T + 1000L, 1, "100", 1));
        assertEquals(IngestStatus.ACCEPTED, b1.status(), "各源位点独立，src-B 的 1 号不能被 src-A 的进度误伤");
        assertEquals(3, p.committedOffset("src-A"));
        assertEquals(1, p.committedOffset("src-B"));
        log.info("committed: src-A={} src-B={}", p.committedOffset("src-A"), p.committedOffset("src-B"));
    }

    @Test
    @DisplayName("并发重复投递多源同号事件：每个 (source,eventId) 恰好计入一次")
    void concurrentDuplicateDeliveryAcrossSources() throws Exception {
        var p = newPipeline();
        int sources = 3;
        int perSource = 20;
        int threads = 8;
        int repeats = 3;

        // 三个来源共用同一组 eventId（"k0".."k19"）与完全相同的业务时间，
        // 模拟“各源独立编号撞号”的极端情况
        List<String> batch = new ArrayList<>();
        for (int s = 0; s < sources; s++) {
            for (int i = 0; i < perSource; i++) {
                batch.add(TestSupport.event("k" + i, "src-" + s, "AAA", "TRADE",
                        T + i * 100L, i, "100", 1));
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int r = 0; r < repeats; r++) {
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
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int distinct = sources * perSource; // 60 个独立的 (source, eventId)
        log.info("并发多源同号: 总投递={} ACCEPTED={} DUPLICATE={} 其它={}",
                results.size(), accepted.get(), duplicate.get(), other.get());

        assertEquals(distinct, accepted.get(), "每个 (source,eventId) 必须恰好接受一次");
        assertEquals(results.size() - distinct, duplicate.get(), "其余投递必须全部判重");
        assertEquals(0, other.get(), "不应出现迟到/非法/回退");

        AggregateView view = p.aggregates().get(0);
        assertEquals(distinct, view.count(), "同号跨源事件必须全部独立计入");
        assertEquals(distinct, view.totalQuantity());
        for (int s = 0; s < sources; s++) {
            assertEquals(perSource - 1, p.committedOffset("src-" + s));
        }
        log.info("聚合: count={} qty={} (期望各 {})", view.count(), view.totalQuantity(), distinct);
    }
}
