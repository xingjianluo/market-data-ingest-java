package com.github.xingjianluo.marketdataingestjava.aggregate;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;
import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按 (symbol, currency, 桶起点) 维护全部聚合桶。
 * 桶键使用嵌套 record；桶计算 floorDiv 以固定时间边界，与到达顺序无关。
 */
public class AggregationTable {

    private record BucketKey(String symbol, String currency, long bucketStart) {
    }

    private final long bucketSizeMs;
    private final Map<BucketKey, BucketAggregate> buckets = new ConcurrentHashMap<>();

    public AggregationTable(long bucketSizeMs) {
        if (bucketSizeMs <= 0) {
            throw new IllegalArgumentException("bucketSizeMs must be > 0");
        }
        this.bucketSizeMs = bucketSizeMs;
    }

    public long bucketSizeMs() {
        return bucketSizeMs;
    }

    public void apply(MarketEvent event) {
        long start = Math.floorDiv(event.eventTime(), bucketSizeMs) * bucketSizeMs;
        BucketKey key = new BucketKey(event.symbol(), event.currency(), start);
        buckets.computeIfAbsent(key, k -> new BucketAggregate(k.symbol(), k.currency(),
                k.bucketStart(), bucketSizeMs)).apply(event);
    }

    public List<AggregateView> allViews() {
        List<AggregateView> views = new ArrayList<>(buckets.size());
        buckets.values().forEach(b -> views.add(b.toView()));
        views.sort(Comparator
                .comparing(AggregateView::symbol)
                .thenComparing(AggregateView::currency)
                .thenComparingLong(AggregateView::bucketStart));
        return views;
    }

    /** 从快照恢复：每个视图重建为一个桶（快照已是按桶聚合的完整结果）。 */
    public void restoreBuckets(List<AggregateView> views) {
        buckets.clear();
        for (AggregateView v : views) {
            BucketKey key = new BucketKey(v.symbol(), v.currency(), v.bucketStart());
            BucketAggregate agg = new BucketAggregate(v.symbol(), v.currency(),
                    v.bucketStart(), v.bucketSizeMs());
            agg.mergeView(v);
            buckets.put(key, agg);
        }
    }
}
