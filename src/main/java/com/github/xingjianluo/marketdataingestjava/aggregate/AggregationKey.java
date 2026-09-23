package com.github.xingjianluo.marketdataingestjava.aggregate;

/** 聚合维度：标的 + 窗口起点。 */
public record AggregationKey(String symbol, long windowStart) {
}
