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
 *   <li>open  = 全序键 {@code (eventTime, source, eventId)} 最小的事件价格；
 *       close = 该键最大的事件价格</li>
 *   <li>时间相同的极端并列：先比 source 字典序，再比来源内 eventId 字典序，
 *       裁决唯一确定，不依赖谁先投递/谁先抢到锁</li>
 *   <li>high / low = 价格最大 / 最小值</li>
 *   <li>count / totalQuantity 为可加计量；quantityByVenue 按 venue 分拆</li>
 * </ul>
 * 因而乱序并入、多源同刻同号、重放重建、快照恢复都收敛到同一结果，可复现。
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
    private String openEventSource;
    private String openEventId;

    private BigDecimal closePrice;
    private long closeEventTime;
    private String closeEventSource;
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
        if (openPrice == null || orderKeyLess(e.eventTime(), e.source(), e.eventId(),
                openEventTime, openEventSource, openEventId)) {
            openPrice = e.price();
            openEventTime = e.eventTime();
            openEventSource = e.source();
            openEventId = e.eventId();
        }
        if (closePrice == null || orderKeyLess(closeEventTime, closeEventSource, closeEventId,
                e.eventTime(), e.source(), e.eventId())) {
            closePrice = e.price();
            closeEventTime = e.eventTime();
            closeEventSource = e.source();
            closeEventId = e.eventId();
        }
    }

    /** 从快照视图合并（恢复用，同样是顺序无关的可加/极值/全序键归并）。 */
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
        if (v.open() != null && (openPrice == null || orderKeyLess(
                v.openEventTime(), v.openEventSource(), v.openEventId(),
                openEventTime, openEventSource, openEventId))) {
            openPrice = v.open();
            openEventTime = v.openEventTime();
            openEventSource = v.openEventSource();
            openEventId = v.openEventId();
        }
        if (v.close() != null && (closePrice == null || orderKeyLess(
                closeEventTime, closeEventSource, closeEventId,
                v.closeEventTime(), v.closeEventSource(), v.closeEventId()))) {
            closePrice = v.close();
            closeEventTime = v.closeEventTime();
            closeEventSource = v.closeEventSource();
            closeEventId = v.closeEventId();
        }
    }

    public AggregateView toView() {
        return new AggregateView(
                symbol, currency, bucketStart, bucketSizeMs,
                count, totalQuantity, openPrice, high, low, closePrice,
                openEventTime, openEventSource, openEventId,
                closeEventTime, closeEventSource, closeEventId,
                new TreeMap<>(quantityByVenue));
    }

    /** 全序比较：(eventTime, source, eventId) 字典序，a 是否严格小于 b。 */
    private static boolean orderKeyLess(long ta, String sourceA, String idA,
                                        long tb, String sourceB, String idB) {
        if (ta != tb) {
            return ta < tb;
        }
        int bySource = sourceA.compareTo(sourceB);
        if (bySource != 0) {
            return bySource < 0;
        }
        return idA.compareTo(idB) < 0;
    }
}
