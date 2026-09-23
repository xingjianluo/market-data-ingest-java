package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationBook;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationKey;
import com.github.xingjianluo.marketdataingestjava.aggregate.WindowAggregation;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedup.Deduplicator;
import com.github.xingjianluo.marketdataingestjava.engine.IngestEngine;
import com.github.xingjianluo.marketdataingestjava.log.EventLog;
import com.github.xingjianluo.marketdataingestjava.offset.CursorTracker;
import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;
import com.github.xingjianluo.marketdataingestjava.protocol.RejectReason;
import com.github.xingjianluo.marketdataingestjava.replay.ReplayService;
import com.github.xingjianluo.marketdataingestjava.state.SnapshotStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.window.LatenessPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 中断恢复与全量重放：不重不漏、位点回退被拒、重放结论逐条一致。 */
class RecoveryReplayTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryReplayTest.class);
    private static final LatenessPolicy POLICY = new LatenessPolicy(10_000, 2_000);

    /** 先处理一批事件（含重复、超期、非法），再用同目录“重启”新引擎，状态必须完整恢复且续处理正确。 */
    @Test
    void snapshotRecoveryResumesExactlyOnce(@TempDir Path dir) throws Exception {
        IngestEngine first = new IngestEngine(POLICY, dir);
        first.ingest(Events.v1("A", 0, "A-0", "BTCUSDT", 1_000, "100", 1));
        first.ingest(Events.v1("A", 1, "A-1", "BTCUSDT", 2_000, "101", 2));
        first.ingest(Events.v1("A", 2, "A-2", "BTCUSDT", 3_000, "102", 3));
        // 重复 + 超期 + 非法
        first.ingest(Events.v2("A", 3, "A-1", "BTCUSDT", 4_000, "0", 9, "SELL"));
        first.ingest(Events.v1("A", 4, "A-4", "BTCUSDT", 0, "1", 1)); // watermark=3000 阈值1000，超期
        first.ingest(Events.invalid("A", 5, "A-5",
                "{\"symbol\":\"X\",\"eventTime\":1,\"price\":1,\"size\":\"big\"}"));

        long snapshotSize = Files.size(dir.resolve("snapshot.json"));
        assertTrue(snapshotSize > 0);

        // 模拟进程重启：新实例从同一目录恢复
        IngestEngine restored = new IngestEngine(POLICY, dir);
        assertEquals(first.cursors(), restored.cursors(), "位点恢复");
        assertEquals(first.watermark(), restored.watermark(), "水位线恢复");
        assertEquals(first.aggregations().keySet(), restored.aggregations().keySet());
        log.info("[恢复] 重启后位点={} watermark={} 窗口={}",
                restored.cursors(), restored.watermark(), restored.aggregations().keySet());

        // 断点续处理：从期望位点 6 继续，不重不漏
        IngestOutcome cont = restored.ingest(Events.v1("A", 6, "A-6", "BTCUSDT", 5_000, "105", 4));
        assertTrue(cont.accepted());
        // 旧事件重放：位点回退必须被明确拒绝
        IngestOutcome backward = restored.ingest(Events.v1("A", 2, "A-2", "BTCUSDT", 3_000, "102", 3));
        assertEquals(RejectReason.OFFSET_BACKWARD, backward.reason());
        // 旧事件用新位点提交：幂等键去重，不重复累计
        IngestOutcome dup = restored.ingest(Events.v1("A", 7, "A-0", "BTCUSDT", 6_000, "1", 1));
        assertEquals(RejectReason.DUPLICATE, dup.reason());

        WindowAggregation before = first.aggregations().get(new AggregationKey("BTCUSDT", 0L));
        WindowAggregation after = restored.aggregations().get(new AggregationKey("BTCUSDT", 0L));
        // 恢复后只新增了 A-6（size=4），重复与回退不累计
        assertEquals(before.totalVolume() + 4, after.totalVolume());
        assertEquals(before.tradeCount() + 1, after.tradeCount());
        log.info("[续处理] volume {} -> {}，count {} -> {}",
                before.totalVolume(), after.totalVolume(), before.tradeCount(), after.tradeCount());
    }

    /** 全量重放：逐条结论与聚合对账完全一致，且重放不修改现网状态。 */
    @Test
    void fullReplayReproducesEveryVerdictAndAggregation(@TempDir Path dir) {
        IngestEngine live = new IngestEngine(POLICY, dir);
        // 两源交错、乱序、含各类拒绝。时间安排保证 B-0/A-2 属于“迟到窗口内并入”而非超期
        live.ingest(Events.v1("A", 0, "A-0", "BTCUSDT", 5_000, "110", 2));
        live.ingest(Events.v1("B", 0, "B-0", "BTCUSDT", 3_500, "90", 1));  // 乱序：阈值3000，并入
        live.ingest(Events.v2("A", 1, "A-1", "ETHUSDT", 6_000, "10", 1, "BUY"));
        live.ingest(Events.v1("A", 2, "A-2", "BTCUSDT", 4_500, "95", 3));  // 乱序：阈值4000，并入
        live.ingest(Events.v2("B", 1, "B-1", "BTCUSDT", 6_500, "111", 4, "SELL"));
        live.ingest(Events.v1("A", 3, "A-0", "BTCUSDT", 7_000, "1", 1));    // 重复
        live.ingest(Events.v1("B", 2, "B-2", "BTCUSDT", 0, "1", 1));        // 超期
        live.ingest(Events.envelope("A", 4, "A-4", "v9",
                "{\"symbol\":\"X\",\"eventTime\":1,\"price\":1,\"size\":1}")); // 非法版本
        live.ingest(Events.v1("A", 5, "A-5", "BTCUSDT", 8_000, "120", 1));

        Map<AggregationKey, WindowAggregation> before = live.aggregations();

        ObjectMapper mapper = new ObjectMapper();
        Path replayDir = dir.resolve("replay");
        EventLog liveLog = new EventLog(dir.resolve("events.jsonl"), mapper);
        var freshFactory = new java.util.function.Supplier<IngestEngine>() {
            @Override
            public IngestEngine get() {
                // 全量重放必须从空状态开始：删除重放目录的残留快照与日志
                try {
                    Files.deleteIfExists(replayDir.resolve("snapshot.json"));
                    Files.deleteIfExists(replayDir.resolve("events.jsonl"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return new IngestEngine(POLICY,
                        new EventCodec(mapper), new CursorTracker(), new Deduplicator(),
                        new AggregationBook(),
                        new SnapshotStore(replayDir.resolve("snapshot.json"), mapper),
                        new EventLog(replayDir.resolve("events.jsonl"), mapper),
                        true);
            }
        };
        ReplayService replayService = new ReplayService(liveLog, freshFactory, live);

        ReplayService.ReplayReport report = replayService.replay();
        log.info("[重放] total={} accepted={} rejected={} verdictMismatches={} aggMatch={}",
                report.total(), report.accepted(), report.rejectedByReason(),
                report.verdictMismatches(), report.aggregationsMatch());

        assertTrue(report.outcomesMatch(), "重放结论与聚合必须完全一致");
        assertEquals(9, report.total());
        assertEquals(6, report.accepted());
        assertEquals(1, report.rejectedByReason().get(RejectReason.DUPLICATE.name()));
        assertEquals(1, report.rejectedByReason().get(RejectReason.LATE.name()));
        assertEquals(1, report.rejectedByReason().get(RejectReason.INVALID_EVENT.name()));
        // 重放幂等：再重放一次结论依旧一致
        ReplayService.ReplayReport second = replayService.replay();
        assertTrue(second.outcomesMatch());

        // 现网状态不被重放改变
        assertEquals(before.keySet(), live.aggregations().keySet());
        assertTrue(Files.exists(dir.resolve("snapshot.json")), "现网快照仍在原位");
        log.info("[重放] 重复重放一致，现网快照未被重放引擎触碰");
    }
}
