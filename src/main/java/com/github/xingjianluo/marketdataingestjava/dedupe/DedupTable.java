package com.github.xingjianluo.marketdataingestjava.dedupe;

import java.util.HashSet;
import java.util.Set;

/**
 * 多来源去重表：以复合键 {@link EventKey}（source, eventId）为幂等键。
 *
 * <p>幂等口径：各来源独立从 1（本系统从 0）编号，跨来源出现相同 eventId、
 * 甚至相同业务时间，都视为各自独立的业务事件分别计入（键中带 source 不会互相吞掉）；
 * 只有同一来源重发同一 eventId 才判定为重复。
 *
 * <p>线程安全。仅记录已被计入聚合（ACCEPTED）的事件；
 * 重复、非法、迟到、回退的事件不会登记，因此修正后重发仍可被接受。
 */
public class DedupTable {

    private final Set<EventKey> seen = new HashSet<>();

    public boolean isDuplicate(EventKey key) {
        synchronized (seen) {
            return seen.contains(key);
        }
    }

    public void markSeen(EventKey key) {
        synchronized (seen) {
            seen.add(key);
        }
    }

    public Set<EventKey> snapshotKeys() {
        synchronized (seen) {
            return new HashSet<>(seen);
        }
    }

    public void restore(Set<EventKey> keys) {
        synchronized (seen) {
            seen.clear();
            seen.addAll(keys);
        }
    }
}
