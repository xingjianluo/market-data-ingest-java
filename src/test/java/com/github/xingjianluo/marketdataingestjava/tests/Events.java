package com.github.xingjianluo.marketdataingestjava.tests;

/** 测试用事件信封构造工具。 */
final class Events {

    private Events() {}

    static String v1(String source, long seq, String eventId, String symbol,
                     long eventTime, String price, long size) {
        return envelope(source, seq, eventId, "v1",
                "{\"symbol\":\"" + symbol + "\",\"eventTime\":" + eventTime
                        + ",\"price\":" + price + ",\"size\":" + size + "}");
    }

    static String v2(String source, long seq, String eventId, String symbol,
                     long eventTime, String price, long size, String side) {
        return envelope(source, seq, eventId, "v2",
                "{\"symbol\":\"" + symbol + "\",\"eventTime\":" + eventTime
                        + ",\"price\":" + price + ",\"size\":" + size
                        + (side == null ? "" : ",\"side\":\"" + side + "\"") + "}");
    }

    static String envelope(String source, long seq, String eventId, String version, String payload) {
        return "{\"source\":\"" + source + "\",\"seq\":" + seq
                + ",\"eventId\":\"" + eventId + "\",\"schemaVersion\":\"" + version
                + "\",\"payload\":" + payload + "}";
    }

    /** 构造 payload 缺字段的非法信封。 */
    static String invalid(String source, long seq, String eventId, String payload) {
        return envelope(source, seq, eventId, "v1", payload);
    }
}
