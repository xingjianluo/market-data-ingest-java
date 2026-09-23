package com.github.xingjianluo.marketdataingestjava.model;

import java.math.BigDecimal;

/**
 * 规范化后的行情事件（与投递版本无关的内部统一模型）。
 *
 * @param schemaVersion 投递结构版本（1 / 2 ...）
 * @param eventTime     业务时间（毫秒时间戳），乱序/迟到判定基于该字段
 * @param sequence      来源内单调递增位点（offset），断点/回退判定基于该字段
 * @param currency       v2 新增字段；v1 缺省固定为 "USD"
 * @param venue          v2 新增字段；v1 缺省固定为 "UNKNOWN"
 */
public record MarketEvent(
        String eventId,
        String source,
        String symbol,
        EventType eventType,
        long eventTime,
        long sequence,
        int schemaVersion,
        BigDecimal price,
        long quantity,
        String currency,
        String venue
) {
}
