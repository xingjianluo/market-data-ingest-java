package com.github.xingjianluo.marketdataingestjava.model;

/**
 * 一条事件的处理结果。reason 给出可解释的判定依据，
 * watermark/committedOffset 回显判定时的来源位点，便于日志核对。
 */
public record IngestResult(
        IngestStatus status,
        String eventId,
        String reason,
        long sourceWatermark,
        long sourceCommittedOffset
) {
    public boolean accepted() {
        return status == IngestStatus.ACCEPTED;
    }
}
