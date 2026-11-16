package com.university.booking.events;

public enum EventStream {
    TYPE("type:"),
    ACTOR("actor:");

    private final String prefix;

    EventStream(String prefix) {
        this.prefix = prefix;
    }

    public String key(String id) {
        return prefix + id;
    }
}
