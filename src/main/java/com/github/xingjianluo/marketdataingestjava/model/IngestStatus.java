package com.github.xingjianluo.marketdataingestjava.model;

/**
 * 单条事件进入摄取管道后的处理结论。五类结论互斥，可用于区分：
 * 正常并入 / 重复 / 非法结构 / 超过迟到窗口 / 位点回退。
 */
public enum IngestStatus {
    ACCEPTED,
    DUPLICATE,
    REJECTED_INVALID,
    REJECTED_LATE,
    REJECTED_OFFSET_REGRESSION
}
