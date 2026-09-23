package com.github.xingjianluo.marketdataingestjava.aggregate;

import com.github.xingjianluo.marketdataingestjava.protocol.MarketEvent;
import com.github.xingjianluo.marketdataingestjava.protocol.TradeSide;
import com.github.xingjianluo.marketdataingestjava.state.WindowAggregationState;

import java.math.BigDecimal;

/**
 * 单个 (symbol, window) 的可变聚合结果。
 *
 * <p>open/close 以事件时间排序确定：同一窗口内 eventTime 最小者为 open、最大者为 close。
 * 同一 eventTime 出现多条时，按稳定顺序键 {@code (source, seq)} 字典序/数值序打破平局，
 * 与事件到达顺序无关，从而保证乱序并入后结果一致、可复现。
 */
public class WindowAggregation {

    private final String symbol;
    private final long windowStart;

    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private long totalVolume;
    private long tradeCount;
    private long buyVolume;
    private long sellVolume;
    private long unknownVolume;

    // 决定 open/close 的稳定顺序键（与到达顺序无关）
    private long openEventTime = Long.MAX_VALUE;
    private String openOrderKey;
    private long closeEventTime = Long.MIN_VALUE;
    private String closeOrderKey;

    public WindowAggregation(String symbol, long windowStart) {
        this.symbol = symbol;
        this.windowStart = windowStart;
    }

    /** 并入一条已通过去重/迟到/位点校验的事件。 */
    public void apply(MarketEvent event) {
        if (!symbol.equals(event.symbol())) {
            throw new IllegalArgumentException("symbol mismatch: window=" + symbol
                    + ", event=" + event.symbol());
        }
        BigDecimal price = event.price();
        long size = event.size();

        if (tradeCount == 0) {
            open = price;
            high = price;
            low = price;
            close = price;
            openEventTime = event.eventTime();
            closeEventTime = event.eventTime();
            openOrderKey = orderKey(event);
            closeOrderKey = orderKey(event);
        } else {
            high = high.max(price);
            low = low.min(price);
            if (isEarlierForOpen(event)) {
                open = price;
                openEventTime = event.eventTime();
                openOrderKey = orderKey(event);
            }
            if (isLaterForClose(event)) {
                close = price;
                closeEventTime = event.eventTime();
                closeOrderKey = orderKey(event);
            }
        }

        totalVolume += size;
        tradeCount++;
        TradeSide side = event.side() == null ? TradeSide.UNKNOWN : event.side();
        switch (side) {
            case BUY -> buyVolume += size;
            case SELL -> sellVolume += size;
            case UNKNOWN -> unknownVolume += size;
        }
    }

    private boolean isEarlierForOpen(MarketEvent event) {
        if (event.eventTime() != openEventTime) {
            return event.eventTime() < openEventTime;
        }
        return orderKey(event).compareTo(openOrderKey) < 0;
    }

    private boolean isLaterForClose(MarketEvent event) {
        if (event.eventTime() != closeEventTime) {
            return event.eventTime() > closeEventTime;
        }
        return orderKey(event).compareTo(closeOrderKey) > 0;
    }

    /**
     * 同一事件时间下的稳定平局键：先按 source 字典序，再按 eventId 字典序。
     * 只用事件不可变业务标识，不用传输 seq（seq 会随投递顺序/重放编号变化），
     * 从而与到达顺序彻底无关。
     */
    private static String orderKey(MarketEvent event) {
        return event.source() + "#" + event.eventId();
    }

    public String symbol() { return symbol; }
    public long windowStart() { return windowStart; }
    public long firstEventTime() { return openEventTime == Long.MAX_VALUE ? 0 : openEventTime; }
    public long lastEventTime() { return closeEventTime == Long.MIN_VALUE ? 0 : closeEventTime; }
    public BigDecimal open() { return open; }
    public BigDecimal high() { return high; }
    public BigDecimal low() { return low; }
    public BigDecimal close() { return close; }
    public long totalVolume() { return totalVolume; }
    public long tradeCount() { return tradeCount; }
    public long buyVolume() { return buyVolume; }
    public long sellVolume() { return sellVolume; }
    public long unknownVolume() { return unknownVolume; }

    long openEventTime() { return openEventTime; }
    String openOrderKey() { return openOrderKey; }
    long closeEventTime() { return closeEventTime; }
    String closeOrderKey() { return closeOrderKey; }

    /** 导出为可序列化状态。 */
    public WindowAggregationState toState() {
        return new WindowAggregationState(symbol, windowStart, open, high, low, close,
                totalVolume, tradeCount, buyVolume, sellVolume, unknownVolume,
                openEventTime, openOrderKey, closeEventTime, closeOrderKey);
    }

    /** 从快照状态重建运行时聚合对象。 */
    public static WindowAggregation fromState(WindowAggregationState s) {
        WindowAggregation a = new WindowAggregation(s.symbol(), s.windowStart());
        a.open = s.open();
        a.high = s.high();
        a.low = s.low();
        a.close = s.close();
        a.totalVolume = s.totalVolume();
        a.tradeCount = s.tradeCount();
        a.buyVolume = s.buyVolume();
        a.sellVolume = s.sellVolume();
        a.unknownVolume = s.unknownVolume();
        a.openEventTime = s.openEventTime();
        a.openOrderKey = s.openOrderKey();
        a.closeEventTime = s.closeEventTime();
        a.closeOrderKey = s.closeOrderKey();
        return a;
    }
}
