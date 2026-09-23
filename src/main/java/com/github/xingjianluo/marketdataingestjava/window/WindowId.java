package com.github.xingjianluo.marketdataingestjava.window;

/** 事件时间滚动窗口标识，起点为 windowSize 的整数倍（epoch millis）。 */
public record WindowId(long start) {

    public static WindowId of(long eventTime, long sizeMillis) {
        if (sizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be positive: " + sizeMillis);
        }
        // Math.floorDiv 保证负 epoch 时间下窗口边界仍稳定（虽然行情时间一般为正）
        return new WindowId(Math.floorDiv(eventTime, sizeMillis) * sizeMillis);
    }
}
