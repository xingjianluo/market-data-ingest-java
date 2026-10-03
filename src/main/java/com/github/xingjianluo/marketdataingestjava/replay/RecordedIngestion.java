package com.github.xingjianluo.marketdataingestjava.replay;

import com.github.xingjianluo.marketdataingestjava.model.IngestResult;

import java.util.Map;

/**
 * 摄取流水账条目：一次投递的完整输入（原始报文 + 续处理期望位点）及其首次处理结论。
 * 记录顺序 = 管道临界区内的实际处理顺序（并发下即锁获取顺序），
 * 该顺序本身是一个确定的串行序，因此按此顺序、以相同输入重放可复现全部判定。
 *
 * @param expectedOffsets 本次投递携带的续处理期望位点（普通摄取为空 Map），
 *                        重放时必须原样带回，否则位点对齐判定无法复现
 */
public record RecordedIngestion(
        long journalIndex,
        String rawJson,
        Map<String, Long> expectedOffsets,
        IngestResult firstResult
) {
}
