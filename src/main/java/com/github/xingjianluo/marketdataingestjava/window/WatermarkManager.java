package com.github.xingjianluo.marketdataingestjava.window;

import java.util.HashMap;
import java.util.Map;

/**
 * 按来源维护事件时间水位 watermark = 已接受事件的最大 eventTime。
 *
 * <p>迟到判定：{@code eventTime < watermark - allowedLatenessMs} 即超期。
 * 边界事件 {@code eventTime == watermark - allowedLatenessMs} 不视为迟到（窗口内闭区间），
 * 保证规则确定、可复现。
 *
 * <p>迟到/重复/非法事件不推进水位；首次见到的来源水位为 Long.MIN_VALUE，首条必接受。
 */
public class WatermarkManager {

    private final long allowedLatenessMs;
    private final Map<String, Long> watermarks = new HashMap<>();

    public WatermarkManager(long allowedLatenessMs) {
        if (allowedLatenessMs < 0) {
            throw new IllegalArgumentException("allowedLatenessMs must be >= 0");
        }
        this.allowedLatenessMs = allowedLatenessMs;
    }

    public long allowedLatenessMs() {
        return allowedLatenessMs;
    }

    public boolean isLate(String source, long eventTime) {
        synchronized (watermarks) {
            Long wm = watermarks.get(source);
            if (wm == null) {
                return false;
            }
            return eventTime < wm - allowedLatenessMs;
        }
    }

    public long watermarkOf(String source) {
        synchronized (watermarks) {
            return watermarks.getOrDefault(source, Long.MIN_VALUE);
        }
    }

    /** 事件被接受后推进水位（仅单调前进）。返回新水位。 */
    public long advance(String source, long eventTime) {
        synchronized (watermarks) {
            long wm = watermarkOf(source);
            long next = Math.max(wm, eventTime);
            watermarks.put(source, next);
            return next;
        }
    }

    public Map<String, Long> snapshotWatermarks() {
        synchronized (watermarks) {
            return new HashMap<>(watermarks);
        }
    }

    public void restore(Map<String, Long> watermarks) {
        synchronized (watermarks) {
            this.watermarks.clear();
            this.watermarks.putAll(watermarks);
        }
    }
}
