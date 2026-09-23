package com.github.xingjianluo.marketdataingestjava.dedupe;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 全局事件去重表：以 eventId 为幂等键。
 * 线程安全。仅记录已被计入聚合（ACCEPTED）的事件；
 * 重复、非法、迟到、回退的事件不会登记，因此修正后重发仍可被接受。
 */
public class DedupTable {

    private final Set<String> seen = new HashSet<>();

    public boolean isDuplicate(String eventId) {
        synchronized (seen) {
            return seen.contains(eventId);
        }
    }

    public void markSeen(String eventId) {
        synchronized (seen) {
            seen.add(eventId);
        }
    }

    public Set<String> snapshotIds() {
        synchronized (seen) {
            return new HashSet<>(seen);
        }
    }

    public void restore(Set<String> ids) {
        synchronized (seen) {
            seen.clear();
            seen.addAll(ids);
        }
    }
}
