package com.github.xingjianluo.marketdataingestjava.state;

/** 快照中一个聚合条目的键值（symbol+窗口起点 -> 聚合状态）。 */
public record AggregationEntry(String symbol, long windowStart, WindowAggregationState state) {
}
