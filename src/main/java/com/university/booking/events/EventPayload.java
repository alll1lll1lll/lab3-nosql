package com.university.booking.events;

import java.util.LinkedHashMap;
import java.util.Map;

public final class EventPayload {

    private EventPayload() {
    }

    public static Map<String, Object> of(Object... keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            Object value = keyValues[i + 1];
            if (value != null) {
                map.put(String.valueOf(keyValues[i]), value instanceof Enum<?> e ? e.name() : value);
            }
        }
        return map;
    }
}
