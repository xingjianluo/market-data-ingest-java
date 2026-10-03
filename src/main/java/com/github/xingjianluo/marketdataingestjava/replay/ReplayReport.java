package com.github.xingjianluo.marketdataingestjava.replay;

import java.util.List;

/**
 * 整体重放核对报告：一共重放多少条、多少条结论一致、多少条不一致，
 * 以及全部不一致明细（哪一条、期望什么、实际什么）。
 */
public record ReplayReport(
        long totalReplayed,
        long matched,
        long mismatched,
        List<ReplayMismatch> mismatches
) {
    public boolean allMatched() {
        return mismatched == 0;
    }

    public String summary() {
        return String.format("重放核对: total=%d matched=%d mismatched=%d",
                totalReplayed, matched, mismatched);
    }
}
