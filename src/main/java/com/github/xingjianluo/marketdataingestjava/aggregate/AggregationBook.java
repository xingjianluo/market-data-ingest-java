package com.github.xingjianluo.marketdataingestjava.aggregate;

import com.github.xingjianluo.marketdataingestjava.protocol.MarketEvent;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** 全部 (symbol,window) 聚合结果的线程安全容器。内部使用单一锁，保证 apply 与快照的原子可见性。 */
public class AggregationBook {

    private final Map<AggregationKey, WindowAggregation> windows = new TreeMap<>(
            (a, b) -> {
                int c = a.symbol().compareTo(b.symbol());
                return c != 0 ? c : Long.compare(a.windowStart(), b.windowStart());
            });

    private final Object lock = new Object();

    public WindowAggregation apply(MarketEvent event, long windowStart) {
        synchronized (lock) {
            AggregationKey key = new AggregationKey(event.symbol(), windowStart);
            WindowAggregation agg = windows.computeIfAbsent(key,
                    k -> new WindowAggregation(k.symbol(), k.windowStart()));
            agg.apply(event);
            return agg;
        }
    }

    public WindowAggregation get(AggregationKey key) {
        synchronized (lock) {
            return windows.get(key);
        }
    }

    public Set<AggregationKey> keys() {
        synchronized (lock) {
            return Set.copyOf(windows.keySet());
        }
    }

    /** 不可变深拷贝快照（聚合对象此刻不会再被外部修改——恢复仅在引擎启动时调用）。 */
    public Map<AggregationKey, WindowAggregation> snapshot() {
        synchronized (lock) {
            return new LinkedHashMap<>(windows);
        }
    }

    /** 仅用于引擎启动恢复：整体替换内存表。 */
    public void restore(Map<AggregationKey, WindowAggregation> data) {
        synchronized (lock) {
            windows.clear();
            windows.putAll(data);
        }
    }
}
