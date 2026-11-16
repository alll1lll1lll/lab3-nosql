package com.university.booking.events;

import java.util.Map;

public record EventDraft(EventType type, String entityId, String actorId, String summary, Map<String, Object> payload) {

    public static EventDraft of(EventType type, String entityId, String actorId, Map<String, Object> payload) {
        return new EventDraft(type, entityId, actorId, type.getTitle(), payload);
    }

    public static EventDraft of(EventType type, String entityId, Map<String, Object> payload) {
        return of(type, entityId, null, payload);
    }
}
