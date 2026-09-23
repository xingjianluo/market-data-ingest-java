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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多源并发接收 + 同幂等键并发重复：只计一次，结果确定。
 *
 * <p>引擎要求每个来源的位点严格连续递增，这与“多源并发”不冲突：不同来源可同时提交，
 * 同一来源在接收端按到达排队、保序投递。本测试用每源单线程工作队列模拟该模型，
 * 多个来源的工作线程在公共线程池上真正并发；重复事件复用已存在 eventId，但使用连续新位点。
 */
class ConcurrentDedupTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentDedupTest.class);

    @Test
    void concurrentDuplicatesCountedExactlyOnce(@TempDir Path dir) throws Exception {
        IngestEngine engine = new IngestEngine(new LatenessPolicy(3_600_000, 60_000), dir);
        int perSource = 200;
        int duplicateRounds = 2; // 每个唯一事件再并发重投 2 次
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> verdicts = new ConcurrentLinkedQueue<>();

        Runnable sourceWorker = () -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        };

        for (String source : new String[]{"A", "B"}) {
            final int sizeBase = source.equals("A") ? 1 : 2;
            pool.submit(() -> {
                sourceWorker.run();
                long seq = 0;
                // 首轮：唯一事件
                for (int i = 0; i < perSource; i++) {
                    String raw = Events.v1(source, seq, source + "-" + i,
                            "BTCUSDT", 1_000 + i, String.valueOf(100 + (i % 50)), sizeBase);
                    IngestOutcome o = engine.ingest(raw);
                    verdicts.add(verdictTag(source, o));
                    seq++;
                }
                // 后续：对同一批 eventId 做并发期内的重复投递（位点仍连续，故被 DUPLICATE 而非 GAP）
                for (int round = 0; round < duplicateRounds; round++) {
                    for (int i = 0; i < perSource; i++) {
                        String raw = Events.v2(source, seq, source + "-" + i,
                                "BTCUSDT", 2_000, "999", 9, "SELL");
                        IngestOutcome o = engine.ingest(raw);
                        verdicts.add(verdictTag(source, o));
                        seq++;
                    }
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发任务应在超时前完成");

        long accepted = verdicts.stream().filter(v -> v.endsWith(":OK")).count();
        long duplicate = verdicts.stream().filter(v -> v.contains(RejectReason.DUPLICATE.name())).count();
        long other = verdicts.size() - accepted - duplicate;
        log.info("[并发] 总提交={} accepted={} duplicate={} other={}",
                verdicts.size(), accepted, duplicate, other);

        assertEquals(2L * perSource, accepted, "只有 2*N 个唯一事件被接受");
        assertEquals(2L * perSource * duplicateRounds, duplicate, "重复投递全部 DUPLICATE");
        assertEquals(0, other, "不应出现其他判定");

        WindowAggregation agg = engine.aggregations().get(new AggregationKey("BTCUSDT", 0L));
        assertEquals(2L * perSource, agg.tradeCount(), "并发下重复不重复计入");
        // A: 200*1, B: 200*2；重复提交的 size=9 一笔都不得进入
        assertEquals(3L * perSource, agg.totalVolume());
        // 两个源真正并发，位点各自连续推进到最后值
        assertEquals((long) perSource * (1 + duplicateRounds) - 1, engine.cursors().get("A"));
        assertEquals((long) perSource * (1 + duplicateRounds) - 1, engine.cursors().get("B"));
        log.info("[并发] 最终聚合 count={} volume={} 位点={} open={} close={}",
                agg.tradeCount(), agg.totalVolume(), engine.cursors(), agg.open(), agg.close());
    }

    private static String verdictTag(String source, IngestOutcome o) {
        return source + ":" + (o.accepted() ? "OK" : o.reason().name());
    }
}
