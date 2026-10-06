package com.github.xingjianluo.marketdataingestjava.web;

/**
 * 整体重放核对请求体（全部字段可选）。
 *
 * <p>{@code resumeFromPosition} 为发起方要求“从哪里接着处理”的流水账位置：
 * 缺省或 0 表示干净起点（一条都还没处理过的位置），是唯一允许的发起位置；
 * 更靠前（回退）或更靠后（跳跃、会漏计）都会被明确拒绝。
 */
public class ReplayVerifyRequest {

    private Long resumeFromPosition;

    public Long getResumeFromPosition() { return resumeFromPosition; }
    public void setResumeFromPosition(Long resumeFromPosition) { this.resumeFromPosition = resumeFromPosition; }
}
