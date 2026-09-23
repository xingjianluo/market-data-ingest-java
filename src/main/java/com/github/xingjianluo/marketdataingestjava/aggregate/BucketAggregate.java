package com.github.xingjianluo.marketdataingestjava.aggregate;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单个时间桶的可变聚合（OHLC + 数量）。合并规则与到达顺序无关：
 * <ul>
 *   <li>open  = (eventTime, eventId) 最小的事件价格；close = 最大的事件价格</li>
 *   <li>high / low = 价格最大 / 最小值</li>
 *   <li>count / totalQuantity 为可加计量；quantityByVenue 按 venue 分拆</li>
 * </ul>
 * 因而乱序并入、重放重建、快照恢复都收敛到同一结果，可复现。
 * 所有方法在桶实例锁内完成，线程安全。
 */
public class BucketAggregate {

    private final String symbol;
    private final String currency;
    private final long bucketStart;
    private final long bucketSizeMs;

    private long count;
    private long totalQuantity;
    private BigDecimal high;
    private BigDecimal low;

    private BigDecimal openPrice;
    private long openEventTime;
    private String openEventId;

    private BigDecimal closePrice;
    private long closeEventTime;
    private String closeEventId;

    private final Map<String, Long> quantityByVenue = new HashMap<>();

    public BucketAggregate(String symbol, String currency, long bucketStart, long bucketSizeMs) {
        this.symbol = symbol;
        this.currency = currency;
        this.bucketStart = bucketStart;
        this.bucketSizeMs = bucketSizeMs;
    }

    public void apply(MarketEvent e) {
        count++;
        totalQuantity += e.quantity();
        quantityByVenue.merge(e.venue(), e.quantity(), Long::sum);

        if (high == null || e.price().compareTo(high) > 0) {
            high = e.price();
        }
        if (low == null || e.price().compareTo(low) < 0) {
            low = e.price();
        }
        if (openPrice == null || earlier(e.eventTime(), e.eventId(), openEventTime, openEventId)) {
            openPrice = e.price();
            openEventTime = e.eventTime();
            openEventId = e.eventId();
        }
        if (closePrice == null || earlier(closeEventTime, closeEventId, e.eventTime(), e.eventId())) {
            closePrice = e.price();
            closeEventTime = e.eventTime();
            closeEventId = e.eventId();
        }
    }

    /** 从快照视图合并（恢复用，同样是顺序无关的可加/极值归并）。 */
    public void mergeView(AggregateView v) {
        count += v.count();
        totalQuantity += v.totalQuantity();
        v.quantityByVenue().forEach((venue, qty) ->
                quantityByVenue.merge(venue, qty, Long::sum));

        if (v.high() != null && (high == null || v.high().compareTo(high) > 0)) {
            high = v.high();
        }
        if (v.low() != null && (low == null || v.low().compareTo(low) < 0)) {
            low = v.low();
        }
        if (v.open() != null && (openPrice == null
                || earlier(v.openEventTime(), v.openEventId(), openEventTime, openEventId))) {
            openPrice = v.open();
            openEventTime = v.openEventTime();
            openEventId = v.openEventId();
        }
        if (v.close() != null && (closePrice == null
                || earlier(closeEventTime, closeEventId, v.closeEventTime(), v.closeEventId()))) {
            closePrice = v.close();
            closeEventTime = v.closeEventTime();
            closeEventId = v.closeEventId();
        }
    }

    public AggregateView toView() {
        return new AggregateView(
                symbol, currency, bucketStart, bucketSizeMs,
                count, totalQuantity, openPrice, high, low, closePrice,
                openEventTime, openEventId, closeEventTime, closeEventId,
                new TreeMap<>(quantityByVenue));
    }

    private static boolean earlier(long t1, String id1, long t2, String id2) {
        if (t1 != t2) {
            return t1 < t2;
        }
        return id1.compareTo(id2) < 0;
    }
}
