package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;

import java.util.Map;

/**
 * 事件日志中的一条记录：链路第一次处理该原始事件时的全部输入与结论。
 *
 * @param ordinal         接收序号（从 0 起，仅用于日志与重放明细定位）
 * @param source          来源；结构非法、解析不出 source 时为 null
 * @param eventId         来源内事件号；解析不出时为 null
 * @param rawJson         原始 JSON 文本（重放的唯一输入，不做任何改写）
 * @param expectedOffsets 首次处理时调用方给出的续处理期望位点（可能为空）
 * @param originalStatus  首次处理结论
 * @param originalReason  首次处理判定理由
 */
public record JournaledEvent(
        int ordinal,
        String source,
        String eventId,
        String rawJson,
        Map<String, Long> expectedOffsets,
        IngestStatus originalStatus,
        String originalReason
) {
}
