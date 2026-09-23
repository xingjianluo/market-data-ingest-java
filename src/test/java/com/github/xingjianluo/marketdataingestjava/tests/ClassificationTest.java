package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.model.IngestResult;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 五类处理结论的可区分性：正常接受、重复、非法、超期迟到、位点回退。
 */
class ClassificationTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(ClassificationTest.class);

    private static final long T0 = 1_000_000L;

    @Test
    @DisplayName("同一事件重复投递只计入一次，结论为 DUPLICATE")
    void duplicateIsCountedOnce() {
        var p = newPipeline();
        String e = TestSupport.event("e1", "src-A", "AAA", "TRADE", T0, 0, "100.00", 10);

        IngestResult first = p.ingest(e);
        IngestResult second = p.ingest(e);

        log.info("首次={} 重复投递={}", first.status(), second.status());
        assertEquals(IngestStatus.ACCEPTED, first.status());
        assertEquals(IngestStatus.DUPLICATE, second.status());
        assertEquals(1, p.aggregates().get(0).count());
    }

    @Test
    @DisplayName("窗口内乱序事件并入既有结果，窗口外事件被拒绝为 LATE")
    void inOrderLateWindowClassification() {
        var p = newPipeline();
        // 先把水位推到 T0+10_000
        IngestResult head = p.ingest(TestSupport.event("e1", "src-A", "AAA", "TRADE",
                T0 + 10_000, 0, "110", 10));
        // 迟到 4s（在 5s 窗口内，边界之内）-> 接受
        IngestResult within = p.ingest(TestSupport.event("e2", "src-A", "AAA", "TRADE",
                T0 + 6_000, 1, "108", 5));
        // 恰好在边界：eventTime = wm - lateness = T0+5_000 -> 不算迟到
        IngestResult boundary = p.ingest(TestSupport.event("e3", "src-A", "AAA", "TRADE",
                T0 + 5_000, 2, "107", 2));
        // 超出窗口 1ms -> 拒绝
        IngestResult tooLate = p.ingest(TestSupport.event("e4", "src-A", "AAA", "TRADE",
                T0 + 4_999, 3, "106", 1));

        log.info("head={} withinWindow={} boundary={} tooLate={} reasons: '{}'",
                head.status(), within.status(), boundary.status(), tooLate.status(),
                tooLate.reason());
        assertEquals(IngestStatus.ACCEPTED, head.status());
        assertEquals(IngestStatus.ACCEPTED, within.status());
        assertEquals(IngestStatus.ACCEPTED, boundary.status());
        assertEquals(IngestStatus.REJECTED_LATE, tooLate.status());
        assertEquals(3, p.aggregates().get(0).count());
        assertNotEquals(tooLate.status(), IngestStatus.DUPLICATE);
    }

    @Test
    @DisplayName("缺字段与类型不匹配被拒绝为 INVALID，不静默丢弃且不落状态")
    void invalidEventsRejectedExplicitly() {
        var p = newPipeline();
        String missingField = TestSupport.raw(
                "{\"eventId\":\"bad1\",\"source\":\"s\",\"symbol\":\"AAA\",\"type\":\"TRADE\","
                        + "\"eventTime\":" + T0 + ",\"sequence\":0,\"price\":100}"); // 缺 quantity
        String wrongType = TestSupport.event("bad2", "s", "AAA", "TRADE", T0, 1, "abc", 10); // price 非数字
        String badEnum = TestSupport.event("bad3", "s", "AAA", "FUND", T0, 2, "100", 10); // 非法 type

        IngestResult r1 = p.ingest(missingField);
        IngestResult r2 = p.ingest(wrongType);
        IngestResult r3 = p.ingest(badEnum);

        log.info("缺字段={} reason='{}'; 类型错误={} reason='{}'; 枚举非法={} reason='{}'",
                r1.status(), r1.reason(), r2.status(), r2.reason(), r3.status(), r3.reason());
        assertEquals(IngestStatus.REJECTED_INVALID, r1.status());
        assertEquals(IngestStatus.REJECTED_INVALID, r2.status());
        assertEquals(IngestStatus.REJECTED_INVALID, r3.status());
        assertEquals(0, p.aggregates().size(), "非法事件不得产生任何聚合");
    }

    @Test
    @DisplayName("位点回退被拒绝为 OFFSET_REGRESSION，且与重复/迟到原因可区分")
    void offsetRegressionRejected() {
        var p = newPipeline();
        p.ingest(TestSupport.event("e1", "src-A", "AAA", "TRADE", T0, 5, "100", 10));
        // 同一 eventId + 旧位点：重复检测优先（已计入），结果是 DUPLICATE
        IngestResult sameId = p.ingest(
                TestSupport.event("e1", "src-A", "AAA", "TRADE", T0, 5, "100", 10));
        // 不同 eventId 但旧位点：位点回退
        IngestResult backtrack = p.ingest(
                TestSupport.event("e-old", "src-A", "AAA", "TRADE", T0, 3, "99", 3));

        log.info("同ID重发={} reason='{}'; 位点回退={} reason='{}'",
                sameId.status(), sameId.reason(), backtrack.status(), backtrack.reason());
        assertEquals(IngestStatus.DUPLICATE, sameId.status());
        assertEquals(IngestStatus.REJECTED_OFFSET_REGRESSION, backtrack.status());
        // 回退事件没有被计入
        assertEquals(1, p.aggregates().get(0).count());
        assertEquals(5, p.committedOffset("src-A"));
    }
}
