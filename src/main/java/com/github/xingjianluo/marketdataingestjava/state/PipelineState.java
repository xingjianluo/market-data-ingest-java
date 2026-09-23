package com.github.xingjianluo.marketdataingestjava.state;

import com.github.xingjianluo.marketdataingestjava.model.AggregateView;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 管道全量状态快照：去重键集合 + 各源水位 + 各源提交位点 + 聚合桶。
 * 该对象是纯数据载体，用于持久化与恢复。
 */
public record PipelineState(
        long savedAtMillis,
        long allowedLatenessMs,
        long bucketSizeMs,
        Set<String> seenEventIds,
        Map<String, Long> watermarks,
        Map<String, Long> committedOffsets,
        List<AggregateView> buckets
) {
}
