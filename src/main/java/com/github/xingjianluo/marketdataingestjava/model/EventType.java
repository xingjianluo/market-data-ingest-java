package com.github.xingjianluo.marketdataingestjava.model;

/** 行情事件类型。 */
public enum EventType {
    TRADE,
    QUOTE;

    public static EventType fromString(String raw) {
        if (raw == null) {
            return null;
        }
        return switch (raw.trim().toUpperCase()) {
            case "TRADE" -> TRADE;
            case "QUOTE" -> QUOTE;
            default -> null;
        };
    }
}
