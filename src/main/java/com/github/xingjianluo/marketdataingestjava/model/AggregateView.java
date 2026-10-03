package com.github.xingjianluo.marketdataingestjava.model;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 单个 (symbol, currency, 时间桶) 聚合的不可变快照。
 *
 * <p>open/close 由全序键 {@code (eventTime, source, eventId)} 决定，与到达顺序、
 * 线程调度无关：时间相同的极端并列下，先按来源标识、再按来源内 eventId 裁决，
 * 并把胜出事件的 source/eventId 一并回显，使“为什么这条排前面”可解释、可复现。
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
        String openEventSource,
        String openEventId,
        long closeEventTime,
        String closeEventSource,
        String closeEventId,
        Map<String, Long> quantityByVenue
) {
}
