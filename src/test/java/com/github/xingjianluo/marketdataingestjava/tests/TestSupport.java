package com.github.xingjianluo.marketdataingestjava.tests;

/** 测试用事件 JSON 构造辅助。 */
final class TestSupport {

    private TestSupport() {
    }

    /** v1 事件（不带 schemaVersion / currency / venue，验证缺省语义）。 */
    static String event(String id, String source, String symbol, String type,
                        long eventTime, long seq, String price, long qty) {
        return String.format(
                "{\"eventId\":\"%s\",\"source\":\"%s\",\"symbol\":\"%s\",\"type\":\"%s\","
                        + "\"eventTime\":%d,\"sequence\":%d,\"price\":%s,\"quantity\":%d}",
                id, source, symbol, type, eventTime, seq, price, qty);
    }

    /** v2 事件（显式版本与新增字段）。 */
    static String eventV2(String id, String source, String symbol, String type,
                          long eventTime, long seq, String price, long qty,
                          String currency, String venue) {
        return String.format(
                "{\"schemaVersion\":2,\"eventId\":\"%s\",\"source\":\"%s\",\"symbol\":\"%s\","
                        + "\"type\":\"%s\",\"eventTime\":%d,\"sequence\":%d,\"price\":%s,"
                        + "\"quantity\":%d,\"currency\":\"%s\",\"venue\":\"%s\"}",
                id, source, symbol, type, eventTime, seq, price, qty, currency, venue);
    }

    /** 手工片段（用于构造缺字段/类型错误等非法事件）。 */
    static String raw(String json) {
        return json;
    }
}
