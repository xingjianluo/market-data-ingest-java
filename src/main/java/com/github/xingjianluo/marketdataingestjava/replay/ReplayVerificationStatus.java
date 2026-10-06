package com.github.xingjianluo.marketdataingestjava.replay;

/**
 * 整体重放核对的发起结论。与摄取逐条结论（ACCEPTED / DUPLICATE /
 * REJECTED_INVALID / REJECTED_LATE / REJECTED_OFFSET_REGRESSION）完全分开命名，
 * 调用方能一眼区分“核对跑完了”与“核对被拒绝”。
 */
public enum ReplayVerificationStatus {

    /** 核对被接受并已完整执行，报告有效。 */
    VERIFIED,

    /**
     * 续处理位置非法，核对被拒绝（一条都没有重放）：
     * 位置比干净起点靠前（回退）或靠后（跳跃、会漏计）都走这个结论，
     * 具体是哪种情况由 reason 明确说明。
     */
    REJECTED_OFFSET_REGRESSION
}
