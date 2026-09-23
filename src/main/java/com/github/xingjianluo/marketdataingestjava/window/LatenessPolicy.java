package com.github.xingjianluo.marketdataingestjava.window;

/**
 * 窗口与迟到判定策略。
 *
 * <p>水位线定义为：所有已接收事件中观察到的最大事件时间（不允许回退）。
 * 当 {@code eventTime < watermark - allowedLatenessMillis} 时判定为超期，
 * 该事件所属窗口视为已封存，必须拒绝。
 *
 * @param windowSizeMillis      滚动窗口长度
 * @param allowedLatenessMillis 允许迟到时长；事件时间 &lt; watermark - allowedLateness 即超期
 */
public record LatenessPolicy(long windowSizeMillis, long allowedLatenessMillis) {

    public LatenessPolicy {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be positive");
        }
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be non-negative");
        }
    }

    /** 事件时间所属窗口起点。 */
    public long windowStart(long eventTime) {
        return WindowId.of(eventTime, windowSizeMillis).start();
    }

    /** 给定当前水位线判断事件是否超期。 */
    public boolean isLate(long eventTime, long watermark) {
        // 水位线初始为 Long.MIN_VALUE（尚无事件），任何真实事件时间都不算超期；
        // 显式判断避免 Long.MIN_VALUE - lateness 的 long 溢出
        if (watermark == Long.MIN_VALUE) {
            return false;
        }
        return eventTime < watermark - allowedLatenessMillis;
    }
}
