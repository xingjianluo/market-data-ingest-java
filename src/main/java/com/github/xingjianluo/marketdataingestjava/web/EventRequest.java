package com.github.xingjianluo.marketdataingestjava.web;

/**
 * 批量摄取请求体。events 为原始 JSON 字符串（逐条经结构校验与版本兼容解析）。
 */
public class EventRequest {

    private java.util.List<String> events = java.util.List.of();
    /** 断点续处理时调用方期望的起始位点（可选，按源给）。 */
    private java.util.Map<String, Long> expectedOffsets = java.util.Map.of();

    public java.util.List<String> getEvents() { return events; }
    public void setEvents(java.util.List<String> events) { this.events = events; }

    public java.util.Map<String, Long> getExpectedOffsets() { return expectedOffsets; }
    public void setExpectedOffsets(java.util.Map<String, Long> expectedOffsets) {
        this.expectedOffsets = expectedOffsets;
    }
}
