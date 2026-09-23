package com.github.xingjianluo.marketdataingestjava.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.protocol.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.protocol.TradeSide;

import java.math.BigDecimal;
import java.util.Set;

/**
 * 接入协议解码器：把原始 JSON 信封解码为规范化 {@link MarketEvent}。
 *
 * <p>协议（本地 JSON over HTTP，见 docs/protocol）：
 * <pre>
 * {
 *   "source": "A",                 // 必填，非空字符串，行情源标识
 *   "seq": 0,                      // 必填，非负整数，来源内连续位点（从 0 起）
 *   "eventId": "A-0",              // 必填，非空字符串，源内唯一
 *   "schemaVersion": "v1",         // 必填，支持 v1 / v2
 *   "payload": {                   // 必填，JSON 对象
 *     "symbol": "BTCUSDT",         // 必填，非空字符串
 *     "eventTime": 1700000000000,  // 必填，非负整数 epoch millis（允许数字字符串）
 *     "price": 101.5,              // 必填，非负数值（允许数字字符串）
 *     "size": 2,                   // 必填，非负整数（允许整数字符串）
 *     "side": "BUY"                // v2 新增，可选：BUY/SELL；缺失或缺省一律稳定取 UNKNOWN
 *   }
 * }
 * </pre>
 *
 * <p>结构演进兼容范围：
 * <ul>
 *   <li>v1 事件没有 {@code side}，解码结果稳定为 {@link TradeSide#UNKNOWN}；v2 事件缺省 side 同理；</li>
 *   <li>支持版本内出现未知字段（前向宽容）：未知字段被忽略，不影响聚合；</li>
 *   <li>不支持的 schemaVersion、必填字段缺失、类型不匹配都不静默丢弃：
 *       返回 {@link Unparseable} 携带明确原因，由引擎推进位点并以 INVALID_EVENT 拒绝。</li>
 * </ul>
 */
public class EventCodec {

    static final Set<String> SUPPORTED_VERSIONS = Set.of("v1", "v2");

    private final ObjectMapper mapper;

    public EventCodec() {
        this(new ObjectMapper());
    }

    public EventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ParseResult parse(String rawJson) {
        JsonNode root;
        try {
            root = mapper.readTree(rawJson);
        } catch (JsonProcessingException e) {
            return new Unparseable(null, null, null,
                    "信封不是合法 JSON: " + e.getOriginalMessage());
        }
        if (root == null || !root.isObject()) {
            return new Unparseable(null, null, null, "信封必须是 JSON 对象");
        }

        String source = readNonBlankText(root, "source");
        Long seq = readSeq(root);
        String eventId = readNonBlankText(root, "eventId");

        String version = root.path("schemaVersion").asText(null);
        if (version == null || version.isBlank()) {
            return new Unparseable(source, seq, eventId,
                    "schemaVersion 缺失或不是字符串（必填，支持 v1/v2）");
        }
        if (!SUPPORTED_VERSIONS.contains(version)) {
            return new Unparseable(source, seq, eventId,
                    "不支持的 schemaVersion=" + version + "（仅支持 v1/v2）");
        }
        if (source == null) {
            return new Unparseable(null, seq, eventId,
                    invalid("source", "非空字符串", root.get("source")));
        }
        if (seq == null) {
            return new Unparseable(source, null, eventId,
                    invalid("seq", "非负整数", root.get("seq")));
        }
        if (eventId == null) {
            return new Unparseable(source, seq, null,
                    invalid("eventId", "非空字符串", root.get("eventId")));
        }

        JsonNode payload = root.get("payload");
        if (payload == null || !payload.isObject()) {
            return new Unparseable(source, seq, eventId,
                    invalid("payload", "JSON 对象", payload));
        }

        String symbol = readNonBlankText(payload, "symbol");
        if (symbol == null) {
            return new Unparseable(source, seq, eventId,
                    invalid("payload.symbol", "非空字符串", payload.get("symbol")));
        }

        Long eventTime = readLongField(payload, "eventTime");
        if (eventTime == null || eventTime < 0) {
            return new Unparseable(source, seq, eventId,
                    invalid("payload.eventTime", "非负整数", payload.get("eventTime")));
        }

        BigDecimal price = readDecimalField(payload, "price");
        if (price == null || price.signum() < 0) {
            return new Unparseable(source, seq, eventId,
                    invalid("payload.price", "非负数值", payload.get("price")));
        }

        Long size = readLongField(payload, "size");
        if (size == null || size < 0) {
            return new Unparseable(source, seq, eventId,
                    invalid("payload.size", "非负整数", payload.get("size")));
        }

        TradeSide side = TradeSide.UNKNOWN;
        if (payload.has("side") && !payload.get("side").isNull()) {
            String sideText = readNonBlankText(payload, "side");
            if (sideText == null) {
                return new Unparseable(source, seq, eventId,
                        invalid("payload.side", "BUY/SELL 字符串", payload.get("side")));
            }
            try {
                side = TradeSide.valueOf(sideText);
            } catch (IllegalArgumentException e) {
                return new Unparseable(source, seq, eventId,
                        "payload.side 取值非法: " + sideText + "（允许 BUY/SELL，缺省为 UNKNOWN）");
            }
        }

        MarketEvent event = MarketEvent.of(source, seq, eventId, symbol, eventTime,
                price, size, side, version);
        return new Parsed(new Envelope(source, seq, eventId, version, payload), event);
    }

    private static String readNonBlankText(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || !v.isTextual()) {
            return null;
        }
        String text = v.asText();
        return text == null || text.isBlank() ? null : text;
    }

    private static Long readSeq(JsonNode root) {
        JsonNode v = root.get("seq");
        if (v == null) {
            return null;
        }
        if (v.isNumber()) {
            return v.isIntegralNumber() && v.canConvertToLong() ? v.longValue() : null;
        }
        if (v.isTextual()) {
            return parseIntegral(v.asText());
        }
        return null;
    }

    private static Long readLongField(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null) {
            return null;
        }
        if (v.isNumber()) {
            return v.isIntegralNumber() && v.canConvertToLong() ? v.longValue() : null;
        }
        if (v.isTextual()) {
            return parseIntegral(v.asText());
        }
        return null;
    }

    private static Long parseIntegral(String text) {
        try {
            BigDecimal bd = new BigDecimal(text.trim());
            return bd.stripTrailingZeros().scale() <= 0 ? bd.longValueExact() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static BigDecimal readDecimalField(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null) {
            return null;
        }
        if (v.isNumber()) {
            return v.decimalValue();
        }
        if (v.isTextual()) {
            try {
                return new BigDecimal(v.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static String invalid(String field, String expected, JsonNode actual) {
        if (actual == null) {
            return field + " 缺失（必填，期望 " + expected + "）";
        }
        return field + " 类型不匹配: 期望 " + expected + ", 实际 "
                + actual.getNodeType() + "(" + truncate(actual.toString()) + ")";
    }

    private static String truncate(String s) {
        return s.length() <= 32 ? s : s.substring(0, 32) + "...";
    }

    /** 解码结果：成功携带信封与规范化事件，失败携带可解析到的定位字段与原因。 */
    public sealed interface ParseResult permits Parsed, Unparseable {}

    public record Parsed(Envelope envelope, MarketEvent event) implements ParseResult {}

    /**
     * @param source  尽力解析到的来源标识（可能为 null）
     * @param seq     尽力解析到的来源位点（可能为 null）
     * @param eventId 尽力解析到的事件标识（可能为 null）
     * @param detail  非法原因（缺失字段/类型不匹配/版本不支持）
     */
    public record Unparseable(String source, Long seq, String eventId, String detail)
            implements ParseResult {}

    /** 原始信封的传输层视图。 */
    public record Envelope(String source, long seq, String eventId,
                           String schemaVersion, JsonNode payload) {}
}
