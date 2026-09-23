package com.github.xingjianluo.marketdataingestjava.dedup;

import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 幂等去重：
 * <ul>
 *   <li>以 {@code (source,eventId)} 为幂等键，重复事件不重复计入；</li>
 *   <li>缓存每个来源“已提交最后位点”的判定结论，用于把最后位点的重投识别为
 *       {@code DUPLICATE}（at-least-once 重试），而把更早位点的回退交给位点检查拒绝；</li>
 *   <li>当来源位点推进时，旧位点的判定缓存被清理，仅保留最后位点，避免无界增长。</li>
 * </ul>
 * 全部操作线程安全。
 */
public class Deduplicator {

    private final Set<String> seenKeys = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Map<String, Map<Long, IngestOutcome>> verdictBySource = new HashMap<>();
    private final Object lock = new Object();

    /** 首次见到该幂等键则登记并返回 true；已存在返回 false。 */
    public boolean firstSeen(String source, String eventId) {
        return seenKeys.add(key(source, eventId));
    }

    /**
     * 位点推进后登记该位点的判定结论，并清理该来源更早位点的缓存。
     */
    public void cacheVerdict(String source, long seq, IngestOutcome outcome) {
        synchronized (lock) {
            Map<Long, IngestOutcome> map = verdictBySource.computeIfAbsent(source, k -> new HashMap<>());
            map.keySet().removeIf(oldSeq -> oldSeq < seq);
            map.put(seq, outcome);
        }
    }

    public Optional<IngestOutcome> cachedVerdict(String source, long seq) {
        synchronized (lock) {
            Map<Long, IngestOutcome> map = verdictBySource.get(source);
            return map == null ? Optional.empty() : Optional.ofNullable(map.get(seq));
        }
    }

    public Set<String> dedupKeys() {
        return Set.copyOf(seenKeys);
    }

    public Map<String, Map<Long, IngestOutcome>> verdictCache() {
        synchronized (lock) {
            Map<String, Map<Long, IngestOutcome>> copy = new LinkedHashMap<>();
            verdictBySource.forEach((s, m) -> copy.put(s, new HashMap<>(m)));
            return copy;
        }
    }

    /** 仅用于引擎启动恢复。 */
    public void restore(Set<String> dedupKeys,
                        Map<String, Map<Long, IngestOutcome>> verdictCache) {
        synchronized (lock) {
            seenKeys.clear();
            seenKeys.addAll(dedupKeys);
            verdictBySource.clear();
            verdictCache.forEach((s, m) -> verdictBySource.put(s, new HashMap<>(m)));
        }
    }

    private static String key(String source, String eventId) {
        return source + "|" + eventId;
    }
}
