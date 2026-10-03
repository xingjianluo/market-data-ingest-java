package com.github.xingjianluo.marketdataingestjava.dedupe;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 事件去重表：幂等键为 (source, eventId) 二元组。
 *
 * <p>多来源各自独立编号，不同来源允许出现相同的 eventId（甚至相同业务时间），
 * 它们是各自独立的业务事件，必须分别计入；只有“同一来源 + 同一 eventId”
 * 的再次投递才判定为重复。
 *
 * <p>线程安全。仅记录已被计入聚合（ACCEPTED）的事件；
 * 重复、非法、迟到、回退的事件不会登记，因此修正后重发仍可被接受。
 */
public class DedupTable {

    /** eventId -> 已计入该 eventId 的来源集合。 */
    private final Map<String, Set<String>> seen = new HashMap<>();

    /** 同一来源、同一 eventId 是否已计入。 */
    public boolean isDuplicate(String source, String eventId) {
        synchronized (seen) {
            Set<String> sources = seen.get(eventId);
            return sources != null && sources.contains(source);
        }
    }

    /**
     * 该 eventId 已被哪些来源计入（不含本次待判定的来源时用于解释
     * “这是别的来源的独立事件”）。无记录时返回空集合。
     */
    public Set<String> sourcesOf(String eventId) {
        synchronized (seen) {
            Set<String> sources = seen.get(eventId);
            return sources == null ? Set.of() : new TreeSet<>(sources);
        }
    }

    public void markSeen(String source, String eventId) {
        synchronized (seen) {
            seen.computeIfAbsent(eventId, k -> new HashSet<>()).add(source);
        }
    }

    /** 快照：eventId -> 来源集合（深度拷贝，键与集合均有序，便于核对）。 */
    public Map<String, Set<String>> snapshot() {
        synchronized (seen) {
            Map<String, Set<String>> copy = new HashMap<>();
            seen.forEach((id, sources) -> copy.put(id, new TreeSet<>(sources)));
            return copy;
        }
    }

    public void restore(Map<String, Set<String>> state) {
        synchronized (seen) {
            seen.clear();
            state.forEach((id, sources) -> seen.put(id, new HashSet<>(sources)));
        }
    }

    /** 已计入的 (source, eventId) 对总数，用于日志/核对。 */
    public int size() {
        synchronized (seen) {
            return seen.values().stream().mapToInt(Set::size).sum();
        }
    }
}
