package com.github.xingjianluo.marketdataingestjava.offset;

import java.util.HashMap;
import java.util.Map;

/**
 * 每来源位点跟踪器。约定每个来源的 seq 从 0 开始、必须连续递增。
 *
 * <ul>
 *   <li>新来源的首条事件 seq 必须为 0，否则视为 {@link Status#GAP}；</li>
 *   <li>{@code seq == expected}：正常推进；</li>
 *   <li>{@code seq == lastCommitted}（最后位点重投）：交给去重器做幂等判定，
 *       原样重投返回 DUPLICATE，内容被篡改则由引擎拒绝；</li>
 *   <li>{@code seq < lastCommitted}：位点回退，明确拒绝（恢复只能从快照位点继续，
 *       或在空状态下全量重放，绝不允许从中间回退）；</li>
 *   <li>{@code seq > expected}：位点跳跃，存在漏投，拒绝。</li>
 * </ul>
 */
public class CursorTracker {

    /** source -> 已提交的最后位点；新来源视为 -1。 */
    private final Map<String, Long> lastCommitted = new HashMap<>();
    private final Object lock = new Object();

    /**
     * @param status        判定类别
     * @param expected      该来源当前期望的下一个位点
     * @param lastCommitted 该来源已提交的最后位点（无则 -1）
     */
    public record CursorCheck(Status status, long expected, long lastCommitted) {}

    public enum Status {
        FIRST,
        ADVANCE,
        RETRY_LAST,
        BACKWARD,
        GAP
    }

    public CursorCheck check(String source, long seq) {
        synchronized (lock) {
            long committed = lastCommitted.getOrDefault(source, -1L);
            long expected = committed + 1;
            Status status;
            if (committed == -1L) {
                status = seq == 0 ? Status.FIRST : Status.GAP;
            } else if (seq == expected) {
                status = Status.ADVANCE;
            } else if (seq == committed) {
                status = Status.RETRY_LAST;
            } else if (seq < committed) {
                status = Status.BACKWARD;
            } else {
                status = Status.GAP;
            }
            return new CursorCheck(status, expected, committed);
        }
    }

    /** 位点推进（合法事件与非法事件都会推进，非法事件以拒绝结论占位，保证源不被卡死）。 */
    public void advance(String source, long seq) {
        synchronized (lock) {
            long committed = lastCommitted.getOrDefault(source, -1L);
            if (seq <= committed) {
                throw new IllegalStateException("cursor cannot move backward: source=" + source
                        + ", committed=" + committed + ", requested=" + seq);
            }
            lastCommitted.put(source, seq);
        }
    }

    public Map<String, Long> positions() {
        synchronized (lock) {
            return new HashMap<>(lastCommitted);
        }
    }

    public long lastCommitted(String source) {
        synchronized (lock) {
            return lastCommitted.getOrDefault(source, -1L);
        }
    }

    /** 仅用于引擎启动恢复。 */
    public void restore(Map<String, Long> positions) {
        synchronized (lock) {
            lastCommitted.clear();
            lastCommitted.putAll(positions);
        }
    }
}
