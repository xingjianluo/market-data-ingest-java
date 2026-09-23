package com.github.xingjianluo.marketdataingestjava.codec;

/**
 * 事件结构非法：缺少必填字段、字段类型不匹配、取值不合法。
 * 此类事件必须被明确拒绝（REJECTED_INVALID），不得静默丢弃。
 */
public class InvalidEventException extends RuntimeException {

    private final String rawEventId;

    public InvalidEventException(String rawEventId, String message) {
        super(message);
        this.rawEventId = rawEventId;
    }

    public String getRawEventId() {
        return rawEventId;
    }
}
