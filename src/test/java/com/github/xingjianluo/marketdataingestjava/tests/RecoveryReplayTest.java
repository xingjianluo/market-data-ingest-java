package com.github.xingjianluo.marketdataingestjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.aggregate.AggregationTable;
import com.github.xingjianluo.marketdataingestjava.checkpoint.CheckpointManager;
import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.dedupe.DedupTable;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.pipeline.IngestPipeline;
import com.github.xingjianluo.marketdataingestjava.state.FileSnapshotRepository;
import com.github.xingjianluo.marketdataingestjava.window.WatermarkManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 处理中断后的状态恢复与重放：
 * 快照恢复后继续处理不重复累计/不漏计；从旧位点重放被明确拒绝；
 * 已提交事件重放天然幂等（DUPLICATE）。
 */
class RecoveryReplayTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryReplayTest.class);
    private static final long T = 60_000L;

    private IngestPipeline rehydrated() {
        ObjectMapper mapper = new ObjectMapper();
        var p = new IngestPipeline(
                new EventCodec(mapper), new DedupTable(),
                new WatermarkManager(LATENESS), new CheckpointManager(),
                new AggregationTable(BUCKET),
                new FileSnapshotRepository(tempDir.resolve("snapshot.json"), mapper));
        p.restore();
        return p;
    }

    @Test
    @DisplayName("快照 -> 重建管道 -> 继续摄入：聚合与位点完全延续，不重复不漏计")
    void snapshotRestoreThenContinue() {
        var p1 = newPipeline();
        for (int i = 0; i < 3; i++) {
            assertEquals(IngestStatus.ACCEPTED, p1.ingest(
                    TestSupport.event("e" + i, "src-A", "AAA", "TRADE",
                            T + 1_000L * i, i, String.valueOf(100 + i), 10)).status());
        }
        p1.snapshot();
        List<AggregateView> before = p1.aggregates();
        log.info("中断前聚合 count={} qty={} committed={}",
                before.get(0).count(), before.get(0).totalQuantity(), p1.committedOffset("src-A"));

        // 模拟进程重启：空内存状态从本地快照恢复
        var p2 = rehydrated();
        assertEquals(before, p2.aggregates(), "恢复后聚合必须逐字段一致");
        assertEquals(2, p2.committedOffset("src-A"));

        // 继续处理 seq=3 的新事件，不重复、不漏计
        IngestResult continued = p2.ingest(
                TestSupport.event("e3", "src-A", "AAA", "TRADE", T + 3_000, 3, "103", 10));
        assertEquals(IngestStatus.ACCEPTED, continued.status());
        AggregateView after = p2.aggregates().get(0);
        log.info("恢复后续处理 count={} qty={} committed={}", after.count(),
                after.totalQuantity(), p2.committedOffset("src-A"));
        assertEquals(4, after.count());
        assertEquals(40, after.totalQuantity());

        // 再次快照并再恢复一次，结果保持稳定（可复现）
        p2.snapshot();
        var p3 = rehydrated();
        assertEquals(after, p3.aggregates().get(0));
        assertEquals(3, p3.committedOffset("src-A"));
    }

    @Test
    @DisplayName("位点回退重放被明确拒绝；从已提交位点的下一位点续处理成功")
    void regressionReplayRefused() {
        var p = newPipeline();
        for (int i = 0; i < 4; i++) {
            p.ingest(TestSupport.event("e" + i, "src-A", "AAA", "TRADE",
                    T + 1_000L * i, i, "100", 10));
        }
        p.snapshot();
        var restarted = rehydrated();
        assertEquals(3, restarted.committedOffset("src-A"));

        // 试图从旧位点 1 开始重放 -> 拒绝（回退）
        String oldEvent = TestSupport.event("e1", "src-A", "AAA", "TRADE", T + 1_000, 1, "100", 10);
        IngestResult regression = restarted.resumeIngest(oldEvent, Map.of("src-A", 1L));
        log.info("回退重放 -> {} reason='{}'", regression.status(), regression.reason());
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION, regression.status());

        // 试图跳过位点 4 直接投 6 -> 拒绝（跳跃会漏计）
        String skip = TestSupport.event("e6", "src-A", "AAA", "TRADE", T + 6_000, 6, "100", 10);
        IngestResult gap = restarted.resumeIngest(skip, Map.of("src-A", 5L));
        log.info("跳跃重放 -> {} reason='{}'", gap.status(), gap.reason());
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION, gap.status());

        // 正确续处理：期望位点 = 已提交位点 3，先重放 e3（已计入，幂等 DUPLICATE），
        // 再消费新事件 e4（seq=4）
        IngestResult replaySeen = restarted.resumeIngest(
                TestSupport.event("e3", "src-A", "AAA", "TRADE", T + 3_000, 3, "100", 10),
                Map.of("src-A", 3L));
        IngestResult next = restarted.resumeIngest(
                TestSupport.event("e4", "src-A", "AAA", "TRADE", T + 4_000, 4, "100", 10),
                Map.of("src-A", 3L));
        log.info("已提交事件重放={} 下一条新事件={}", replaySeen.status(), next.status());
        assertEquals(IngestStatus.DUPLICATE, replaySeen.status());
        assertEquals(IngestStatus.ACCEPTED, next.status());
        assertEquals(5, restarted.aggregates().get(0).count());
    }

    @Test
    @DisplayName("无快照时恢复为空启动，不报错")
    void restoreWithoutSnapshotIsEmpty() {
        var p = newPipeline();
        p.restore();
        assertEquals(0, p.aggregates().size());
        assertEquals(CheckpointManager.NO_OFFSET, p.committedOffset("whatever"));
    }
}
