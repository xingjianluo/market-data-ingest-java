package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 乱序/重复摄入下，聚合 OHLC 结果只由事件集合决定、与到达顺序无关；
 * 多次独立运行得到逐字段一致的结果（可复现）。
 *
 * <p>事件时间跨度 0~5s，恰为允许迟到窗口（闭区间），故任意顺序到达都应被接受。
 */
class OrderIndependenceTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(OrderIndependenceTest.class);

    private static final long T0 = 60_000L;

    // index -> {eventTimeOffset, price}
    private static final long[][] SPEC = {
            {5_000, 110},
            {3_000, 103},
            {4_000, 107},
            {0, 101},
            {2_500, 105},
            {1_000, 102},
            {1_500, 106},
    };

    private AggregateView runInPermutation(List<Integer> permutation) {
        var p = newPipeline();
        long seq = 0; // seq 按投递顺序单调分配，与聚合值无关
        for (int idx : permutation) {
            p.ingest(TestSupport.event("k" + idx, "src-A", "AAA", "TRADE",
                    T0 + SPEC[idx][0], seq++, String.valueOf(SPEC[idx][1]), 100));
        }
        return p.aggregates().get(0);
    }

    @Test
    @DisplayName("不同到达顺序（含乱序）产生完全一致的 OHLC 聚合")
    void permutationsProduceSameAggregate() {
        List<Integer> order = new ArrayList<>(List.of(0, 1, 2, 3, 4, 5, 6));

        AggregateView baseline = runInPermutation(order);
        Collections.shuffle(order, new java.util.Random(42));
        AggregateView shuffled1 = runInPermutation(order);
        Collections.shuffle(order, new java.util.Random(7));
        AggregateView shuffled2 = runInPermutation(order);

        log.info("baseline : open={} high={} low={} close={} qty={} openId={} closeId={}",
                baseline.open(), baseline.high(), baseline.low(), baseline.close(),
                baseline.totalQuantity(), baseline.openEventId(), baseline.closeEventId());
        log.info("shuffle42: open={} high={} low={} close={} qty={}",
                shuffled1.open(), shuffled1.high(), shuffled1.low(), shuffled1.close(),
                shuffled1.totalQuantity());

        // open=最早事件(0ms,id=k3)=101；close=最晚事件(5s,id=k0)=110；high=110 low=101
        assertEquals("101", baseline.open().toPlainString());
        assertEquals("110", baseline.close().toPlainString());
        assertEquals("110", baseline.high().toPlainString());
        assertEquals("101", baseline.low().toPlainString());
        assertEquals(700, baseline.totalQuantity());
        assertEquals(7, baseline.count());
        assertEquals("k3", baseline.openEventId());
        assertEquals("k0", baseline.closeEventId());

        assertEquals(baseline, shuffled1, "乱序到达聚合必须一致");
        assertEquals(baseline, shuffled2, "乱序到达聚合必须一致");
    }
}
