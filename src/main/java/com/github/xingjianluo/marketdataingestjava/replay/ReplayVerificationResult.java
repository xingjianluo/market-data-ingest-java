package com.github.xingjianluo.marketdataingestjava.replay;

/**
 * 一次整体重放核对的发起结果。
 *
 * <p>两种互斥形态：
 * <ul>
 *   <li>{@link ReplayVerificationStatus#VERIFIED}：核对从干净起点完整执行，
 *       {@link #report()} 给出 总数/一致/不一致 统计与不一致明细；</li>
 *   <li>{@link ReplayVerificationStatus#REJECTED_OFFSET_REGRESSION}：续处理位置非法，
 *       一条都没有重放，{@link #report()} 为 null，{@link #reason()} 说明拒绝原因。
 *       该结论与“正常核对通过、重复、其它拒绝”在 status 上一眼可分。</li>
 * </ul>
 */
public record ReplayVerificationResult(
        ReplayVerificationStatus status,
        String reason,
        ReplayReport report
) {
    public static ReplayVerificationResult verified(ReplayReport report) {
        return new ReplayVerificationResult(ReplayVerificationStatus.VERIFIED,
                "verified from clean start (从干净起点整体重放完成)", report);
    }

    public static ReplayVerificationResult rejected(String reason) {
        return new ReplayVerificationResult(ReplayVerificationStatus.REJECTED_OFFSET_REGRESSION,
                reason, null);
    }

    /** 核对是否被接受并执行。 */
    public boolean verifiedOk() {
        return status == ReplayVerificationStatus.VERIFIED;
    }
}
