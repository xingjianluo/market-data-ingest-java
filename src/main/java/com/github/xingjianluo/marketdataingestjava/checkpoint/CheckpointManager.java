package com.github.xingjianluo.marketdataingestjava.checkpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * 按来源维护提交位点（已接受事件的最大 sequence）。
 *
 * <p>约定来源内 sequence 从 0 起单调递增，未见来源的提交位点为 -1。
 * 回退判定：{@code sequence <= committedOffset} 即为回退/重复投递，明确拒绝，
 * 绝不从旧位点继续累计，避免重复计入。
 */
public class CheckpointManager {

    /** 尚未见到任何事件的来源，其提交位点。 */
    public static final long NO_OFFSET = -1;

    private final Map<String, Long> offsets = new HashMap<>();

    public long committedOffsetOf(String source) {
        synchronized (offsets) {
            return offsets.getOrDefault(source, NO_OFFSET);
        }
    }

    public boolean isRegression(String source, long sequence) {
        return sequence <= committedOffsetOf(source);
    }

    public void commit(String source, long sequence) {
        synchronized (offsets) {
            offsets.put(source, Math.max(committedOffsetOf(source), sequence));
        }
    }

    public Map<String, Long> snapshotOffsets() {
        synchronized (offsets) {
            return new HashMap<>(offsets);
        }
    }

    public void restore(Map<String, Long> sourceOffsets) {
        synchronized (offsets) {
            offsets.clear();
            offsets.putAll(sourceOffsets);
        }
    }
}
