package com.github.xingjianluo.marketdataingestjava.tests;

import com.github.xingjianluo.marketdataingestjava.codec.EventCodec;
import com.github.xingjianluo.marketdataingestjava.protocol.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.protocol.TradeSide;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 结构演进与结构非法场景：缺省语义稳定、非法事件不静默丢弃并给出原因。 */
class EventCodecTest {

    private static final Logger log = LoggerFactory.getLogger(EventCodecTest.class);
    private final EventCodec codec = new EventCodec();

    private static String envelope(String source, long seq, String eventId,
                                  String version, String payload) {
        return "{\"source\":\"" + source + "\",\"seq\":" + seq
                + ",\"eventId\":\"" + eventId + "\",\"schemaVersion\":\"" + version
                + "\",\"payload\":" + payload + "}";
    }

    @Test
    void v1MissingSideDefaultsToUnknownAndSameBusinessFieldsAsV2() {
        String v1 = envelope("A", 0, "A-0", "v1",
                "{\"symbol\":\"BTCUSDT\",\"eventTime\":1000,\"price\":101.5,\"size\":2}");
        String v2 = envelope("A", 0, "A-0", "v2",
                "{\"symbol\":\"BTCUSDT\",\"eventTime\":1000,\"price\":101.5,\"size\":2,\"side\":\"BUY\"}");

        MarketEvent e1 = assertParsed(v1);
        MarketEvent e2 = assertParsed(v2);

        assertEquals(TradeSide.UNKNOWN, e1.side(), "v1 无 side，缺省必须稳定为 UNKNOWN");
        assertEquals(TradeSide.BUY, e2.side());
        assertEquals(e1.symbol(), e2.symbol());
        assertEquals(e1.eventTime(), e2.eventTime());
        assertEquals(0, e1.price().compareTo(e2.price()));
        assertEquals(e1.size(), e2.size());
        log.info("[演进] v1 缺省 side -> {}；v2 显式 side -> {}；同业务字段聚合语义一致", e1.side(), e2.side());
    }

    @Test
    void v2MissingSideAlsoDefaultsToUnknown() {
        String v2 = envelope("A", 0, "A-1", "v2",
                "{\"symbol\":\"BTCUSDT\",\"eventTime\":1000,\"price\":10,\"size\":1}");
        MarketEvent event = assertParsed(v2);
        assertEquals(TradeSide.UNKNOWN, event.side());
        log.info("[演进] v2 缺省 side 同样稳定取 UNKNOWN eventId={}", event.eventId());
    }

    @Test
    void unknownFieldsAreIgnoredWithinSupportedVersion() {
        String raw = envelope("A", 0, "A-2", "v2",
                "{\"symbol\":\"BTCUSDT\",\"eventTime\":1000,\"price\":10,\"size\":1,"
                        + "\"venue\":\"XYZ\",\"futureField\":{\"a\":1}}");
        MarketEvent event = assertParsed(raw);
        assertEquals("BTCUSDT", event.symbol());
        log.info("[演进] 支持版本内的未知字段被前向宽容忽略 eventId={}", event.eventId());
    }

    @Test
    void missingRequiredFieldsAndTypeMismatchAreRejectedWithReason() {
        assertUnparseable(envelope("A", 0, "A-3", "v1",
                "{\"eventTime\":1000,\"price\":10,\"size\":1}"), "symbol");
        assertUnparseable(envelope("A", 0, "A-4", "v1",
                "{\"symbol\":\"X\",\"eventTime\":1000,\"price\":\"abc\",\"size\":1}"), "price");
        assertUnparseable(envelope("A", 0, "A-5", "v1",
                "{\"symbol\":\"X\",\"eventTime\":\"later\",\"price\":10,\"size\":1}"), "eventTime");
        assertUnparseable(envelope("A", 0, "A-6", "v1",
                "{\"symbol\":\"X\",\"eventTime\":1000,\"price\":10,\"size\":-1}"), "size");
        assertUnparseable(envelope("A", 0, "A-7", "v9",
                "{\"symbol\":\"X\",\"eventTime\":1000,\"price\":10,\"size\":1}"), "schemaVersion");
        assertUnparseable(envelope("A", 0, "A-8", "v1",
                "{\"symbol\":\"X\",\"eventTime\":1000,\"price\":10,\"size\":1,\"side\":\"WHALE\"}"), "side");
        assertUnparseable("{not-json", "JSON");
    }

    @Test
    void numericStringsAcceptedPerProtocol() {
        String raw = envelope("A", 0, "A-9", "v2",
                "{\"symbol\":\"X\",\"eventTime\":\"1000\",\"price\":\"101.25\",\"size\":\"3\"}");
        MarketEvent event = assertParsed(raw);
        assertEquals(0, event.price().compareTo(new BigDecimal("101.25")));
        assertEquals(3, event.size());
        log.info("[协议] 数字字符串按协议接受: price={} size={}", event.price(), event.size());
    }

    private MarketEvent assertParsed(String raw) {
        EventCodec.ParseResult result = codec.parse(raw);
        log.info("[解码] raw={} -> {}", raw.length() > 90 ? raw.substring(0, 90) + "..." : raw,
                result instanceof EventCodec.Parsed p ? "Parsed" : result);
        assertInstanceOf(EventCodec.Parsed.class, result);
        return ((EventCodec.Parsed) result).event();
    }

    private void assertUnparseable(String raw, String reasonFragment) {
        EventCodec.ParseResult result = codec.parse(raw);
        assertInstanceOf(EventCodec.Unparseable.class, result);
        EventCodec.Unparseable bad = (EventCodec.Unparseable) result;
        assertTrue(bad.detail().contains(reasonFragment),
                "拒绝原因应包含 '" + reasonFragment + "'，实际: " + bad.detail());
        log.info("[非法] 未静默丢弃 source={} seq={} eventId={} detail={}",
                bad.source(), bad.seq(), bad.eventId(), bad.detail());
    }
}
