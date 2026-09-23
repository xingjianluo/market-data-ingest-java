package com.github.xingjianluo.marketdataingestjava.protocol;

import java.math.BigDecimal;

/**
 * 解码后的规范化行情事件，与接入版本无关：v1 缺省字段在解码阶段已被填充为确定值。
 *
 * @param source        行情源标识（幂等键的一部分）
 * @param seq           来源内单调位点（外层 envelope 的 seq）
 * @param eventId       源内事件唯一标识（幂等键的一部分）
 * @param symbol        合约/标的代码
 * @param eventTime     事件时间（epoch millis）
 * @param price         成交价
 * @param size          成交量
 * @param side          成交方向，v1 事件稳定缺省为 UNKNOWN
 * @param schemaVersion 事件结构版本（如 v1、v2）
 */
public record MarketEvent(String source, long seq, String eventId, String symbol,
                          long eventTime, BigDecimal price, long size,
                          TradeSide side, String schemaVersion) {

    /** 由解码器构造规范化事件。 */
    public static MarketEvent of(String source, long seq, String eventId, String symbol,
                                 long eventTime, BigDecimal price, long size,
                                 TradeSide side, String schemaVersion) {
        return new MarketEvent(source, seq, eventId, symbol, eventTime, price, size,
                side == null ? TradeSide.UNKNOWN : side, schemaVersion);
    }
}
