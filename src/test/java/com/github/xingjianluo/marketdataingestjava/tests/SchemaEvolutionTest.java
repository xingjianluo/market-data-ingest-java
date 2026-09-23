package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.codec.InvalidEventException;
import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件结构演进：v1（缺新字段）与 v2（带新字段）在同一聚合下结论一致、可复现；
 * 缺字段/类型错误不静默丢弃。
 */
class SchemaEvolutionTest extends PipelineTest {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionTest.class);
    private static final long T = 60_000L;

    @Test
    @DisplayName("v1 缺失字段取稳定缺省：currency=USD, venue=UNKNOWN, version=1")
    void v1MissingFieldsGetStableDefaults() {
        EventCodec codec = new EventCodec(new ObjectMapper());
        MarketEvent e = codec.decode(TestSupport.event("e1", "s", "AAA", "TRADE", T, 0, "100", 10));

        log.info("v1 解析结果: version={} currency={} venue={}", e.schemaVersion(), e.currency(), e.venue());
        assertEquals(1, e.schemaVersion());
        assertEquals("USD", e.currency());
        assertEquals("UNKNOWN", e.venue());
    }

    @Test
    @DisplayName("v1 与 v2 同一 symbol/currency 聚合同桶；不同 currency 分桶，结果确定")
    void mixedVersionsAggregateConsistently() {
        var p = newPipeline();
        // v1（currency 缺省 USD），v2 USD，v2 EUR
        assertEquals(IngestStatus.ACCEPTED, p.ingest(
                TestSupport.event("e1", "s", "AAA", "TRADE", T + 1_000, 0, "100", 10)).status());
        assertEquals(IngestStatus.ACCEPTED, p.ingest(
                TestSupport.eventV2("e2", "s", "AAA", "TRADE", T + 2_000, 1, "102", 5,
                        "USD", "NYSE")).status());
        assertEquals(IngestStatus.ACCEPTED, p.ingest(
                TestSupport.eventV2("e3", "s", "AAA", "TRADE", T + 3_000, 2, "101", 7,
                        "EUR", "NYSE")).status());

        List<AggregateView> views = p.aggregates();
        views.forEach(v -> log.info("聚合桶: {} {} start={} count={} high={} low={} venues={}",
                v.symbol(), v.currency(), v.bucketStart(), v.count(), v.high(), v.low(),
                v.quantityByVenue()));

        assertEquals(2, views.size(), "USD 与 EUR 必须分桶");
        AggregateView usd = views.stream().filter(v -> v.currency().equals("USD")).findFirst().orElseThrow();
        AggregateView eur = views.stream().filter(v -> v.currency().equals("EUR")).findFirst().orElseThrow();
        assertEquals(2, usd.count());
        // v1 缺省 venue 计入 UNKNOWN，v2 显式 NYSE
        assertEquals(10L, usd.quantityByVenue().get("UNKNOWN"));
        assertEquals(5L, usd.quantityByVenue().get("NYSE"));
        assertEquals(1, eur.count());
    }

    @Test
    @DisplayName("新增字段存在但类型错误必须拒绝，不得静默回退缺省值")
    void mistypedNewFieldRejected() {
        var p = newPipeline();
        String badCurrency = TestSupport.raw(
                "{\"schemaVersion\":2,\"eventId\":\"x1\",\"source\":\"s\",\"symbol\":\"AAA\","
                        + "\"type\":\"TRADE\",\"eventTime\":" + T + ",\"sequence\":0,"
                        + "\"price\":100,\"quantity\":1,\"currency\":123}");
        var result = p.ingest(badCurrency);
        log.info("currency 类型错误 -> {} reason='{}'", result.status(), result.reason());
        assertEquals(IngestStatus.REJECTED_INVALID, result.status());
        assertTrue(result.reason().contains("currency"));
    }

    @Test
    @DisplayName("缺省语义是稳定常量：解析器直接暴露缺省常量且与文档一致")
    void defaultsAreStableConstants() {
        assertEquals("USD", EventCodec.DEFAULT_CURRENCY);
        assertEquals("UNKNOWN", EventCodec.DEFAULT_VENUE);
        assertThrows(InvalidEventException.class, () ->
                new EventCodec(new ObjectMapper()).decode("not-json"));
    }
}
