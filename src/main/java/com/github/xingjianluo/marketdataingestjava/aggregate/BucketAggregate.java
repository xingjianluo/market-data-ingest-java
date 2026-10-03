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
 *   <li>open  = 确定性全序 (eventTime, source, eventId) 最小的事件价格；
 *       close = 最大的事件价格</li>
 *   <li>并列取舍：先比 eventTime；再比 source 字典序；再比 eventId 字典序。
 *       多来源各自独立编号时，“同时刻 + 同 eventId”的跨源并列也有确定结论，
 *       且 openEventSource/closeEventSource 会把取舍依据回显出来，可解释</li>
 *   <li>high / low = 价格最大 / 最小值</li>
 *   <li>count / totalQuantity 为可加计量；quantityByVenue 按 venue 分拆</li>
 * </ul>
 * 因而乱序并入、多源并发、重放重建、快照恢复都收敛到同一结果，可复现。
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
    private String openEventSource;

    private BigDecimal closePrice;
    private long closeEventTime;
    private String closeEventId;
    private String closeEventSource;

    private final Map<String, Long> quantityByVenue = new HashMap<>();

    public BucketAggregate(String symbol, String currency, long bucketStart, long bucketSizeMs) {
        this.symbol = symbol;
        this.currency = currency;
        this.bucketStart = bucketStart;
        this.bucketSizeMs = bucketSizeMs;
    }

    public synchronized void apply(MarketEvent e) {
        count++;
        totalQuantity += e.quantity();
        quantityByVenue.merge(e.venue(), e.quantity(), Long::sum);

        if (high == null || e.price().compareTo(high) > 0) {
            high = e.price();
        }
        if (low == null || e.price().compareTo(low) < 0) {
            low = e.price();
        }
        if (openPrice == null || earlier(e.eventTime(), e.source(), e.eventId(),
                openEventTime, openEventSource, openEventId)) {
            openPrice = e.price();
            openEventTime = e.eventTime();
            openEventSource = e.source();
            openEventId = e.eventId();
        }
        if (closePrice == null || earlier(closeEventTime, closeEventSource, closeEventId,
                e.eventTime(), e.source(), e.eventId())) {
            closePrice = e.price();
            closeEventTime = e.eventTime();
            closeEventSource = e.source();
            closeEventId = e.eventId();
        }
    }

    /** 从快照视图合并（恢复用，同样是顺序无关的可加/极值归并）。 */
    public synchronized void mergeView(AggregateView v) {
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
                || earlier(v.openEventTime(), v.openEventSource(), v.openEventId(),
                        openEventTime, openEventSource, openEventId))) {
            openPrice = v.open();
            openEventTime = v.openEventTime();
            openEventSource = v.openEventSource();
            openEventId = v.openEventId();
        }
        if (v.close() != null && (closePrice == null
                || earlier(closeEventTime, closeEventSource, closeEventId,
                        v.closeEventTime(), v.closeEventSource(), v.closeEventId()))) {
            closePrice = v.close();
            closeEventTime = v.closeEventTime();
            closeEventSource = v.closeEventSource();
            closeEventId = v.closeEventId();
        }
    }

    public synchronized AggregateView toView() {
        return new AggregateView(
                symbol, currency, bucketStart, bucketSizeMs,
                count, totalQuantity, openPrice, high, low, closePrice,
                openEventTime, openEventId, openEventSource,
                closeEventTime, closeEventId, closeEventSource,
                new TreeMap<>(quantityByVenue));
    }

    /**
     * 确定性全序：(eventTime, source, eventId) 三元组字典序。
     * 返回 true 表示键1排在键2之前（“更早”）。
     */
    private static boolean earlier(long t1, String src1, String id1,
                                   long t2, String src2, String id2) {
        if (t1 != t2) {
            return t1 < t2;
        }
        int bySource = src1.compareTo(src2);
        if (bySource != 0) {
            return bySource < 0;
        }
        return id1.compareTo(id2) < 0;
    }
}
