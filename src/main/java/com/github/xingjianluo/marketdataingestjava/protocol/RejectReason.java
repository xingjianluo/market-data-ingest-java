package com.github.xingjianluo.marketdataingestjava.protocol;

/**
 * 事件被拒绝的互斥原因分类。任何“未计入”的事件都必须落到其中一种，
 * 以便与重复、非法、超期等场景明确区分。
 */
public enum RejectReason {
    /** 幂等键 (source,eventId) 已处理过，或重投的是已提交位点上的同一条事件。 */
    DUPLICATE("重复事件，不重复计入"),
    /** 事件时间落在水位线减去允许迟到窗口之前，对应窗口已封存。 */
    LATE("超过允许迟到窗口，事件被拒绝"),
    /** 位点回退：seq 小于来源已提交位点且不是最后位点的重投。 */
    OFFSET_BACKWARD("位点回退被拒绝"),
    /** 位点跳跃：seq 大于来源期望位点，存在漏投。 */
    OFFSET_GAP("位点跳跃被拒绝"),
    /** 结构非法：版本不支持 / 必填字段缺失 / 类型不匹配。 */
    INVALID_EVENT("事件结构非法");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
