package com.github.xingjianluo.marketdataingestjava.state;

import com.github.xingjianluo.marketdataingestjava.protocol.IngestOutcome;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 引擎全量状态快照，用于断点恢复：位点、水位线、幂等键、最后位点判定缓存、聚合结果。
 * 字段全部为可直接 JSON 序列化的稳定类型。
 */
public record EngineSnapshot(
        Map<String, Long> cursors,
        long watermark,
        Set<String> dedupKeys,
        Map<String, Map<Long, IngestOutcome>> verdictCache,
        List<AggregationEntry> aggregations) {

    public static EngineSnapshot empty() {
        return new EngineSnapshot(Map.of(), Long.MIN_VALUE, Set.of(), Map.of(), List.of());
    }
}
