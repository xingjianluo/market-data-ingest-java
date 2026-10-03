package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;

/**
 * 重放逐条比对中的一条不一致明细：哪一条（ordinal/source/eventId）、
 * 期望（首次处理）什么结论与理由、重放实际得到什么。
 */
public record ReplayMismatch(
        int ordinal,
        String source,
        String eventId,
        IngestStatus expectedStatus,
        IngestStatus actualStatus,
        String expectedReason,
        String actualReason
) {
}
