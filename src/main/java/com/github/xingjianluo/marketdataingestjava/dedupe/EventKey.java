package com.github.xingjianluo.marketdataingestjava.dedupe;

import com.github.xingjianluo.marketdataingestjava.model.MarketEvent;

/**
 * 多来源幂等键：(source, eventId) 复合键。
 *
 * <p>口径：各来源独立编号，同 eventId 跨来源是各自独立的业务事件，必须分别计入；
 * 只有同一 source 下同一 eventId 重发才算重复。该 record 即该口径的唯一载体。
 */
public record EventKey(String source, String eventId) {

    public static EventKey of(MarketEvent event) {
        return new EventKey(event.source(), event.eventId());
    }

    public static EventKey of(String source, String eventId) {
        return new EventKey(source, eventId);
    }
}
