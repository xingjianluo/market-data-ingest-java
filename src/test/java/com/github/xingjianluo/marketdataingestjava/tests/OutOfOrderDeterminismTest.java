package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.window.LatenessPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 乱序确定性：同一事件集合，无论按事件时间正序、逆序还是随机洗牌提交，
 * 只要所有事件都在允许迟到窗口内，最终聚合结果必须完全一致、可复现。
 */
class OutOfOrderDeterminismTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderDeterminismTest.class);

    private static final long W = 60_000;
    private static final long L = 60_000; // 宽松迟到窗口：所有乱序都允许并入
    private static final int N = 30;

    private List<String> eventSet() {
        List<String> events = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            // 相同事件时间用于验证平局打破：time 1000 上有多条
            long time = 1_000 + (i % 7) * 100;
            String side = i % 3 == 0 ? "BUY" : i % 3 == 1 ? "SELL" : null;
            events.add(Events.v2("A", i, "A-" + i, "BTCUSDT", time,
                    String.valueOf(100 + i), i % 5 + 1, side));
        }
        return events;
    }

    @Test
    void differentArrivalOrdersProduceSameAggregation(@TempDir Path dir) {
        List<String> ascending = eventSet();

        List<String> descending = new ArrayList<>(ascending);
        Collections.reverse(descending);

        List<String> shuffled = new ArrayList<>(ascending);
        Collections.shuffle(shuffled, new java.util.Random(42));
        List<String> shuffled2 = new ArrayList<>(ascending);
        Collections.shuffle(shuffled2, new java.util.Random(7));

        WindowAggregation a = runOrder(dir.resolve("a"), ascending);
        WindowAggregation b = runOrder(dir.resolve("b"), descending);
        WindowAggregation c = runOrder(dir.resolve("c"), shuffled);
        WindowAggregation d = runOrder(dir.resolve("d"), shuffled2);

        assertSameAggregation(a, b);
        assertSameAggregation(a, c);
        assertSameAggregation(a, d);
        log.info("[顺序无关] 正序/逆序/洗牌 开高低收={}/{}/{}/{} count={} volume={} buy={} sell={} unknown={}",
                a.open(), a.high(), a.low(), a.close(), a.tradeCount(), a.totalVolume(),
                a.buyVolume(), a.sellVolume(), a.unknownVolume());
    }

    private WindowAggregation runOrder(Path dir, List<String> events) {
        IngestEngine engine = new IngestEngine(new LatenessPolicy(W, L), dir);
        // seq 是源内传输位点，必须按提交顺序连续；乱序改变的是 payload.eventTime，不是 seq
        for (int i = 0; i < events.size(); i++) {
            engine.ingest(resequence(events.get(i), i));
        }
        return engine.aggregations().get(new AggregationKey("BTCUSDT", 0L));
    }

    /** 把信封外层 seq 替换为提交顺序号，事件其余内容（含 eventTime/eventId）保持不变。 */
    private static String resequence(String raw, long newSeq) {
        return raw.replaceFirst("\"seq\":\\d+", "\"seq\":" + newSeq);
    }

    private void assertSameAggregation(WindowAggregation x, WindowAggregation y) {
        assertEquals(x.open().compareTo(y.open()), 0);
        assertEquals(x.high().compareTo(y.high()), 0);
        assertEquals(x.low().compareTo(y.low()), 0);
        assertEquals(x.close().compareTo(y.close()), 0);
        assertEquals(x.totalVolume(), y.totalVolume());
        assertEquals(x.tradeCount(), y.tradeCount());
        assertEquals(x.buyVolume(), y.buyVolume());
        assertEquals(x.sellVolume(), y.sellVolume());
        assertEquals(x.unknownVolume(), y.unknownVolume());
    }
}
