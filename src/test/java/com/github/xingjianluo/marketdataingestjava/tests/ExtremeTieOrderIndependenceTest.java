package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 极端并列下的顺序无关性：不同来源在同一时刻、用相同 eventId 投递事件时，
 * open/close 由确定性全序 (eventTime, source, eventId) 决定——
 * 先比业务时间，并列比来源字典序，再并列比 eventId 字典序。
 * 无论投递顺序如何打乱、线程谁先抢到锁，聚合结果逐字段一致，
 * 且 openEventSource/closeEventSource 回显取舍依据，判定理由可解释。
 */
class ExtremeTieOrderIndependenceTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(ExtremeTieOrderIndependenceTest.class);
    private static final long T0 = 60_000L;

    /** {source, eventId, eventTimeOffset, price} —— 大量“同时刻 + 同号”跨源并列。
     *  同一来源内 eventId 不重复（否则属同源重发），并列只发生在跨源之间。 */
    private static final String[][] SPEC = {
            {"src-B", "e1", "0", "101"},    // 最早时刻并列（B）
            {"src-A", "e1", "0", "102"},    // 最早时刻并列（A）-> 全序最小，应为 open
            {"src-C", "e2", "1000", "103"}, // 中间时刻三源并列
            {"src-D", "e2", "1000", "104"},
            {"src-A", "e2", "1000", "105"},
            {"src-D", "e3", "2000", "106"}, // 最晚时刻并列（D）-> 全序最大，应为 close
            {"src-C", "e3", "2000", "107"}, // 最晚时刻并列（C）
    };

    /** 按给定投递顺序跑一遍；seq 按各源投递顺序分配（聚合与 seq 无关）。 */
    private AggregateView runInOrder(List<Integer> order) {
        var p = newPipeline();
        Map<String, Long> seqBySource = new HashMap<>();
        for (int idx : order) {
            String[] s = SPEC[idx];
            long seq = seqBySource.merge(s[0], 1L, Long::sum) - 1;
            p.ingest(TestSupport.event(s[1], s[0], "AAA", "TRADE",
                    T0 + Long.parseLong(s[2]), seq, s[3], 100));
        }
        return p.aggregates().get(0);
    }

    @Test
    @DisplayName("同时刻+同号跨源并列：open/close 按 (eventTime,source,eventId) 全序取舍，理由可解释")
    void extremeTiesResolvedByDeterministicTotalOrder() {
        AggregateView view = runInOrder(List.of(0, 1, 2, 3, 4, 5, 6));

        log.info("open={} (time={} source={} id={}) close={} (time={} source={} id={}) high={} low={}",
                view.open(), view.openEventTime(), view.openEventSource(), view.openEventId(),
                view.close(), view.closeEventTime(), view.closeEventSource(), view.closeEventId(),
                view.high(), view.low());

        // open：最早时刻 T0+0 上 src-A 与 src-B 并列同号，source 字典序 A<B -> src-A 的 102
        assertEquals("102", view.open().toPlainString());
        assertEquals("src-A", view.openEventSource(), "并列取舍依据必须回显：source 字典序小者为 open");
        assertEquals("e1", view.openEventId());
        // close：最晚时刻 T0+2000 上 src-C 与 src-D 并列同号，D>C -> src-D 的 106
        assertEquals("106", view.close().toPlainString());
        assertEquals("src-D", view.closeEventSource(), "并列取舍依据必须回显：source 字典序大者为 close");
        assertEquals("e3", view.closeEventId());

        assertEquals("107", view.high().toPlainString());
        assertEquals("101", view.low().toPlainString());
        assertEquals(7, view.count());
        assertEquals(700, view.totalQuantity());
    }

    @Test
    @DisplayName("同一批并列事件随机打乱 20 次：聚合逐字段一致")
    void shuffledDeliveriesProduceIdenticalAggregate() {
        AggregateView baseline = runInOrder(List.of(0, 1, 2, 3, 4, 5, 6));
        for (int seed = 1; seed <= 20; seed++) {
            List<Integer> order = new ArrayList<>(List.of(0, 1, 2, 3, 4, 5, 6));
            Collections.shuffle(order, new Random(seed));
            AggregateView view = runInOrder(order);
            assertEquals(baseline, view, "打乱顺序(seed=" + seed + ")后聚合必须逐字段一致");
        }
        log.info("20 次随机打乱均与 baseline 逐字段一致: open={}({}/{}) close={}({}/{})",
                baseline.open(), baseline.openEventSource(), baseline.openEventId(),
                baseline.close(), baseline.closeEventSource(), baseline.closeEventId());
    }

    @Test
    @DisplayName("4 源各自一线程并发抢投并列事件，重复 10 轮：聚合与串行 baseline 逐字段一致")
    void concurrentTieDeliveryIsDeterministic() throws Exception {
        AggregateView baseline = runInOrder(List.of(0, 1, 2, 3, 4, 5, 6));
        // 每源一个线程、源内按 seq 顺序投递（符合“来源内位点单调”的接入约定），
        // 源与源之间并发抢锁——正是“同时刻同号谁先到”的不确定来源。
        Map<String, List<Integer>> bySourceIdx = new java.util.LinkedHashMap<>();
        for (int i = 0; i < SPEC.length; i++) {
            bySourceIdx.computeIfAbsent(SPEC[i][0], k -> new ArrayList<>()).add(i);
        }
        for (int round = 0; round < 10; round++) {
            var p = newPipeline();
            ExecutorService pool = Executors.newFixedThreadPool(bySourceIdx.size());
            CountDownLatch start = new CountDownLatch(1);
            for (var entry : bySourceIdx.entrySet()) {
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    long seq = 0;
                    for (int i : entry.getValue()) {
                        String[] s = SPEC[i];
                        p.ingest(TestSupport.event(s[1], s[0], "AAA", "TRADE",
                                T0 + Long.parseLong(s[2]), seq++, s[3], 100));
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            assertEquals(baseline, p.aggregates().get(0),
                    "并发抢投第 " + round + " 轮结果必须与串行一致");
        }
        log.info("10 轮并发抢投均与串行 baseline 逐字段一致（线程调度不影响并列取舍）");
    }
}
