package com.github.xingjianluo.marketdataingestjava.model;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 单个 (symbol, currency, 时间桶) 聚合的不可变快照。
 * open/close 以 (eventTime, eventId) 二元组决定，与到达顺序无关，保证可复现。
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
        long closeEventTime,
        String closeEventId,
        Map<String, Long> quantityByVenue
) {
}
