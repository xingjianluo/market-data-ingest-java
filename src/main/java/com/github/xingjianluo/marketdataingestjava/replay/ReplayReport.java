package com.github.xingjianluo.marketdataingestjava.replay;

import java.util.List;

/**
 * 整体重放核对报告：重放条数、结论一致/不一致条数、逐条不一致明细，
 * 以及重放结束时聚合视图是否与当前链路逐字段一致。
 */
public record ReplayReport(
        int totalReplayed,
        int matched,
        int mismatched,
        boolean aggregatesMatch,
        List<ReplayMismatch> mismatches
) {
    public boolean allMatched() {
        return mismatched == 0 && aggregatesMatch;
    }
}
