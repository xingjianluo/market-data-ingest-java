package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.model.IngestStatus;

/**
 * 一条重放结论与首次处理结论不一致的明细：
 * 哪一条（流水账序号 + eventId）、期望什么、实际什么。
 */
public record ReplayMismatch(
        long journalIndex,
        String eventId,
        IngestStatus expectedStatus,
        IngestStatus actualStatus,
        String expectedReason,
        String actualReason,
        long expectedWatermark,
        long actualWatermark,
        long expectedCommittedOffset,
        long actualCommittedOffset
) {
    @Override
    public String toString() {
        return String.format(
                "journal#%d eventId=%s 期望[status=%s watermark=%d committed=%d reason='%s'] "
                        + "实际[status=%s watermark=%d committed=%d reason='%s']",
                journalIndex, eventId,
                expectedStatus, expectedWatermark, expectedCommittedOffset, expectedReason,
                actualStatus, actualWatermark, actualCommittedOffset, actualReason);
    }
}
