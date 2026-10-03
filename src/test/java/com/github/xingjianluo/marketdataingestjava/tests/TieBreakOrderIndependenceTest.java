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
 * 场景二：极端并列下的顺序无关。
 *
 * <p>不同来源在同一业务时刻、又恰好用了一样的来源内编号（eventId）时，
 * open/close 这类与先后有关的指标不能随投递顺序、抢锁结果变化。
 * 裁决规则是全序键 {@code (eventTime, source, eventId)}：
 * 时间相同先按 source 字典序，再按来源内 eventId 字典序，胜出者唯一确定。
 */
class TieBreakOrderIndependenceTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(TieBreakOrderIndependenceTest.class);
    private static final long T = 60_000L;

    // 四条事件：业务时间完全相同、eventId 完全相同（各自来源内都是 "1"），仅 source/价格不同
    private record Spec(String source, String price) {
    }

    private static final List<Spec> SPEC = List.of(
            new Spec("exchange-C", "103"),
            new Spec("exchange-A", "101"),
            new Spec("exchange-B", "102"),
            new Spec("exchange-D", "104"));

    private AggregateView runShuffled(List<Integer> order) {
        var p = newPipeline();
        long seq = 0;
        for (int idx : order) {
            Spec s = SPEC.get(idx);
            // eventId 全部用 "1"，eventTime 全部相同 T；不同源 seq 各自独立从 0 起
            p.ingest(TestSupport.event("1", s.source(), "AAA", "TRADE",
                    T, 0, s.price(), 10));
            seq++;
        }
        return p.aggregates().get(0);
    }

    @Test
    @DisplayName("同时刻同编号跨源事件：open/close 由 (time,source,eventId) 唯一裁决，打乱顺序逐字段一致")
    void extremeTieResolvedBySourceThenIdIndependentlyOfDeliveryOrder() {
        List<Integer> order = new ArrayList<>(List.of(0, 1, 2, 3));

        AggregateView baseline = runShuffled(order);
        Collections.shuffle(order, new java.util.Random(101));
        AggregateView shuffled1 = runShuffled(order);
        Collections.shuffle(order, new java.util.Random(202));
        AggregateView shuffled2 = runShuffled(order);
        // 反向顺序再跑一遍
        AggregateView reversed = runShuffled(new ArrayList<>(List.of(3, 2, 1, 0)));

        log.info("baseline open={}@{}({}) close={}@{}({}) high={} low={} count={}",
                baseline.open(), baseline.openEventTime(), baseline.openEventSource(),
                baseline.close(), baseline.closeEventTime(), baseline.closeEventSource(),
                baseline.high(), baseline.low(), baseline.count());
        log.info("一次随机打乱投递顺序: {}", order);

        // 全序键最小 = (T, exchange-A, 1) -> open 必为 101；
        // 最大 = (T, exchange-D, 1) -> close 必为 104；与谁先投递无关。
        assertEquals("101", baseline.open().toPlainString());
        assertEquals("exchange-A", baseline.openEventSource());
        assertEquals("1", baseline.openEventId());
        assertEquals("104", baseline.close().toPlainString());
        assertEquals("exchange-D", baseline.closeEventSource());
        assertEquals("1", baseline.closeEventId());
        assertEquals("104", baseline.high().toPlainString());
        assertEquals("101", baseline.low().toPlainString());
        assertEquals(4, baseline.count());

        assertEquals(baseline, shuffled1, "打乱投递顺序后 OHLC 必须逐字段一致");
        assertEquals(baseline, shuffled2, "再次打乱仍必须一致");
        assertEquals(baseline, reversed, "完全反向投递仍必须一致");
        log.info("三次乱序/反向投递聚合逐字段一致，裁决理由：同刻时 source 字典序 exchange-A 最先、exchange-D 最后");
    }

    @Test
    @DisplayName("同刻同源时再用来源内 eventId 裁决：source 平局由 eventId 兜底")
    void sameSourceSameTimeFallsBackToEventId() {
        var p = newPipeline();
        // 同一来源、同一时刻两条不同 eventId（seq 顺序递增），open 取 id 字典序更小者
        p.ingest(TestSupport.event("z9", "exchange-A", "AAA", "TRADE", T, 0, "120", 1));
        p.ingest(TestSupport.event("a1", "exchange-A", "AAA", "TRADE", T, 1, "100", 1));

        AggregateView v = p.aggregates().get(0);
        log.info("同源同刻: open={} openId={} close={} closeId={}",
                v.open(), v.openEventId(), v.close(), v.closeEventId());
        assertEquals("100", v.open().toPlainString());
        assertEquals("a1", v.openEventId());
        assertEquals("120", v.close().toPlainString());
        assertEquals("z9", v.closeEventId());
    }
}
