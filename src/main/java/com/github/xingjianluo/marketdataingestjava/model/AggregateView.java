package com.github.xingjianluo.marketdataingestjava.model;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 单个 (symbol, currency, 时间桶) 聚合的不可变快照。
 *
 * <p>open/close 以确定性全序 (eventTime, source, eventId) 决定：
 * 先比业务时间，并列时比来源字典序，再并列比 eventId 字典序；
 * 最小者为 open，最大者为 close。多来源各自独立编号时，
 * “同时刻 + 同 eventId”的跨源并列也能给出确定、可解释、与投递顺序无关的取舍。
 */
public record AggregateView(
        String symbol,
        String currency,
        long bucketStart,
        long bucketSizeMs,
        long count,
        long totalQuantity,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        long openEventTime,
        String openEventId,
        String openEventSource,
        long closeEventTime,
        String closeEventId,
        String closeEventSource,
        Map<String, Long> quantityByVenue
) {
}
