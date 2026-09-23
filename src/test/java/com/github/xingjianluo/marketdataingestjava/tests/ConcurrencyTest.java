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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多源并发 + 重复并发：同一批事件被多个线程重复投递，
 * 每个 eventId 必须恰好被计入一次，最终聚合确定、与线程调度无关。
 */
class ConcurrencyTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);
    private static final long T = 60_000L;

    @Test
    @DisplayName("8 线程并发重复投递同一批事件：恰好每个 eventId 计入一次")
    void concurrentDuplicatesCountedOnce() throws Exception {
        var p = newPipeline();
        int sources = 4;
        int perSource = 50;
        int threads = 8;
        int repeats = 4;

        // 为每个源构造顺序事件（seq 单调，时间小幅乱序但在迟到窗口内）
        List<String> batch = new ArrayList<>();
        for (int s = 0; s < sources; s++) {
            for (int i = 0; i < perSource; i++) {
                long time = T + (i * 100L);
                batch.add(TestSupport.event("ev-" + s + "-" + i, "src-" + s,
                        "SYM" + (s % 2), "TRADE", time, i, "100", 1));
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

        int distinct = sources * perSource;
        log.info("并发投递完成: 总投递={} ACCEPTED={} DUPLICATE={} 其它拒绝={}",
                results.size(), accepted.get(), duplicate.get(), other.get());

        assertEquals(distinct, accepted.get(), "每个 eventId 必须恰好接受一次");
        assertEquals(results.size() - distinct, duplicate.get());
        assertEquals(0, other.get(), "不应出现迟到/非法/回退");
        // 位点必须精确推进到每源最后一条
        for (int s = 0; s < sources; s++) {
            assertEquals(perSource - 1, p.committedOffset("src-" + s));
        }
        // 聚合总量确定：每桶 = 每源 50 条 * 每事件数量 1，两个 symbol
        long totalQty = p.aggregates().stream().mapToLong(v -> v.totalQuantity()).sum();
        assertEquals(distinct, totalQty);
        log.info("聚合桶数={} 总成交量={}", p.aggregates().size(), totalQty);
    }

    @Test
    @DisplayName("并发结果可复现：两次独立并发运行的聚合视图逐字段相等")
    void concurrentRunsAreDeterministic() throws Exception {
        var view1 = runConcurrentOnce(11);
        var view2 = runConcurrentOnce(23);
        assertEquals(view1, view2, "两次并发运行结果必须一致（可复现）");
        log.info("两次并发运行聚合视图一致，桶数={}", view1.size());
    }

    private List<?> runConcurrentOnce(int seed) throws Exception {
        var p = newPipeline();
        // 并发模型：多源并发，每源内部按 seq 顺序（每源对应一个分区/线程）。
        int sources = 4;
        int perSource = 100;
        ExecutorService pool = Executors.newFixedThreadPool(sources);
        CountDownLatch start = new CountDownLatch(1);
        for (int src = 0; src < sources; src++) {
            final int s = src;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int i = 0; i < perSource; i++) {
                    // 不同源用不同的到达起始抖动，但事件时间跨度始终 < 迟到窗口
                    long time = T + i * 10L + (s % 3) * 3L;
                    String json = TestSupport.event("d-" + s + "-" + i, "src-" + s,
                            "AAA", "TRADE", time, i, String.valueOf(100 + (i % 5)), 2);
                    p.ingest(json);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        return p.aggregates();
    }
}
