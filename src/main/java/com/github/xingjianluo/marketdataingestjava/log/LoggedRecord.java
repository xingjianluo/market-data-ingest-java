package com.github.xingjianluo.marketdataingestjava.log;

import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;

/**
 * 事件日志中的一条记录：原始信封与当时的判定结论。
 * 保留原始结论使全量重放可以逐条比对“判定是否与首次处理完全一致”。
 */
public record LoggedRecord(String raw, IngestOutcome outcome, long recordedAt) {
}
