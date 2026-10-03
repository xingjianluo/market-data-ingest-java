package com.github.xingjianluo.marketdataingestjava.web;

import java.util.Map;

/**
 * 整体重放请求体（全部字段可选）：startOffsets 给出各源期望的续处理起始位置。
 * 干净起点只接受 -1；缺省（空）即从干净起点无对齐重放。
 */
public class ReplayRequest {

    private Map<String, Long> startOffsets = Map.of();

    public Map<String, Long> getStartOffsets() {
        return startOffsets;
    }

    public void setStartOffsets(Map<String, Long> startOffsets) {
        this.startOffsets = startOffsets;
    }
}
