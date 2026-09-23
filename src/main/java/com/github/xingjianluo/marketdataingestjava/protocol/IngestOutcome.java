package com.github.xingjianluo.marketdataingestjava.protocol;

/**
 * 一次摄取的判定结论，accepted=false 时必须带互斥的 {@link RejectReason}。
 *
 * @param accepted    是否计入聚合
 * @param reason      拒绝原因（接受时为 null）
 * @param source      行情源
 * @param seq         来源位点
 * @param eventId     事件标识（无法解析时可为 null）
 * @param symbol      标的（无法解析时可为 null）
 * @param windowStart 落入的窗口起点（被拒绝时可为 null）
 * @param watermark   判定时的全局水位线
 * @param detail      人可读的判定依据
 */
public record IngestOutcome(boolean accepted, RejectReason reason, String source, long seq,
                            String eventId, String symbol, Long windowStart,
                            long watermark, String detail) {

    public static IngestOutcome accepted(String source, long seq, String eventId,
                                         String symbol, long windowStart, long watermark,
                                         String detail) {
        return new IngestOutcome(true, null, source, seq, eventId, symbol,
                windowStart, watermark, detail);
    }

    public static IngestOutcome rejected(RejectReason reason, String source, long seq,
                                         String eventId, String symbol, Long windowStart,
                                         long watermark, String detail) {
        return new IngestOutcome(false, reason, source, seq, eventId, symbol,
                windowStart, watermark, detail);
    }
}
