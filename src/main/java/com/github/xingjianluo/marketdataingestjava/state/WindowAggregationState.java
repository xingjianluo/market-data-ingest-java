package com.github.xingjianluo.marketdataingestjava.state;

import java.math.BigDecimal;

/**
 * 单个窗口聚合结果的可序列化状态。与运行时可变类解耦，
 * 保证快照 JSON 的字段集合显式、稳定（结构演进时可独立做兼容）。
 */
public record WindowAggregationState(
        String symbol,
        long windowStart,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        long totalVolume,
        long tradeCount,
        long buyVolume,
        long sellVolume,
        long unknownVolume,
        long openEventTime,
        String openOrderKey,
        long closeEventTime,
        String closeOrderKey) {
}
