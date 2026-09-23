package com.github.xingjianluo.marketdataingestjava.protocol;

/** 成交方向。v1 事件无此字段，解码时取稳定缺省值 {@link #UNKNOWN}。 */
public enum TradeSide {
    BUY,
    SELL,
    UNKNOWN
}
