package com.github.xingjianluo.marketdataingestjava.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdataingestjava.model.EventType;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * 负责原始 JSON -> 规范化 MarketEvent 的解析与结构演进兼容。
 *
 * <p>结构演进兼容范围：
 * <ul>
 *   <li>v1 必填：eventId, source, symbol, type, price, quantity, eventTime, sequence</li>
 *   <li>v2 新增：currency（缺省 "USD"）、venue（缺省 "UNKNOWN"）；新增字段“缺省即默认值”，语义稳定</li>
 *   <li>schemaVersion 缺省按 1 处理（旧消息不带版本）</li>
 * </ul>
 *
 * <p>字段缺失或类型不匹配一律抛 {@link InvalidEventException}，绝不静默丢弃。
 */
public class EventCodec {

    /** 新增字段缺省语义（稳定常量，不可随环境变化）。 */
    public static final String DEFAULT_CURRENCY = "USD";
    public static final String DEFAULT_VENUE = "UNKNOWN";
    public static final int DEFAULT_SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public EventCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 解析并校验。非法（缺字段/类型不匹配/取值非法）抛 InvalidEventException。
     */
    public MarketEvent decode(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            throw new InvalidEventException(null, "raw payload is null or blank");
        }
        final JsonNode root;
        try {
            root = objectMapper.readTree(rawJson);
        } catch (IOException e) {
            throw new InvalidEventException(null, "payload is not valid JSON: " + e.getMessage());
        }
        if (root == null || !root.isObject()) {
            throw new InvalidEventException(null, "payload root must be a JSON object");
        }

        String eventId = requireText(root, "eventId");
        String source = requireText(root, "source");
        String symbol = requireText(root, "symbol");

        String typeRaw = requireText(root, "type");
        EventType eventType = EventType.fromString(typeRaw);
        if (eventType == null) {
            throw new InvalidEventException(eventId,
                    "field 'type' has unsupported value '" + typeRaw + "' (expect TRADE|QUOTE)");
        }

        BigDecimal price = requireDecimal(root, "price", eventId);
        if (price.signum() <= 0) {
            throw new InvalidEventException(eventId, "field 'price' must be > 0, got " + price);
        }
        long quantity = requireLong(root, "quantity", eventId);
        if (quantity <= 0) {
            throw new InvalidEventException(eventId, "field 'quantity' must be > 0, got " + quantity);
        }
        long eventTime = requireLong(root, "eventTime", eventId);
        long sequence = requireLong(root, "sequence", eventId);
        if (sequence < 0) {
            throw new InvalidEventException(eventId, "field 'sequence' must be >= 0, got " + sequence);
        }

        int schemaVersion = root.has("schemaVersion") && !root.get("schemaVersion").isNull()
                ? (int) requireLong(root, "schemaVersion", eventId)
                : DEFAULT_SCHEMA_VERSION;
        if (schemaVersion < 1) {
            throw new InvalidEventException(eventId, "field 'schemaVersion' must be >= 1, got " + schemaVersion);
        }

        // v2 新增字段：缺失 -> 稳定缺省；存在但类型错误 -> 明确拒绝。
        String currency = optionalText(root, "currency", DEFAULT_CURRENCY, eventId);
        String venue = optionalText(root, "venue", DEFAULT_VENUE, eventId);

        return new MarketEvent(eventId, source, symbol, eventType, eventTime, sequence,
                schemaVersion, price, quantity, currency, venue);
    }

    private static String requireText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new InvalidEventException(peekId(root), "missing required field '" + field + "'");
        }
        if (!node.isTextual() || node.asText().isBlank()) {
            throw new InvalidEventException(peekId(root),
                    "field '" + field + "' must be a non-empty string");
        }
        return node.asText();
    }

    private static String optionalText(JsonNode root, String field, String defaultValue, String eventId) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        if (!node.isTextual()) {
            throw new InvalidEventException(eventId, "field '" + field + "' must be a string if present");
        }
        return node.asText();
    }

    private static long requireLong(JsonNode root, String field, String eventId) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new InvalidEventException(eventId, "missing required field '" + field + "'");
        }
        if (!node.isIntegralNumber()) {
            throw new InvalidEventException(eventId,
                    "field '" + field + "' must be an integer, got " + node.getNodeType());
        }
        return node.longValue();
    }

    private static BigDecimal requireDecimal(JsonNode root, String field, String eventId) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new InvalidEventException(eventId, "missing required field '" + field + "'");
        }
        if (!node.isNumber()) {
            throw new InvalidEventException(eventId,
                    "field '" + field + "' must be a number, got " + node.getNodeType());
        }
        return node.decimalValue();
    }

    private static String peekId(JsonNode root) {
        JsonNode id = root.get("eventId");
        return id != null && id.isTextual() ? id.asText() : null;
    }
}
