package com.github.xingjianluo.marketdataingestjava.state;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 管道全量状态快照：去重表（eventId -> 已计入来源集合）+ 各源水位 + 各源提交位点 + 聚合桶。
 * 幂等键为 (source, eventId)，快照按 eventId 分组存来源集合，恢复后语义不变。
 * 该对象是纯数据载体，用于持久化与恢复。
 */
public record PipelineState(
        long savedAtMillis,
        long allowedLatenessMs,
        long bucketSizeMs,
        Map<String, Set<String>> seenEventIds,
        Map<String, Long> watermarks,
        Map<String, Long> committedOffsets,
        List<AggregateView> buckets
) {
}
