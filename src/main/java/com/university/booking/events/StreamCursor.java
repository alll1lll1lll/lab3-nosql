package com.university.booking.events;

import com.university.booking.dto.event.EventView;
import com.university.booking.exception.ValidationException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

record StreamCursor(Instant at, UUID id) {

    static StreamCursor of(EventView event) {
        return new StreamCursor(event.occurredAt().toInstant(), event.eventId());
    }

    private static int compare(StreamCursor a, StreamCursor b) {
        int time = a.at.compareTo(b.at);
        if (time != 0) {
            return time;
        }
        int uuidTime = Long.compare(a.id.timestamp(), b.id.timestamp());
        if (uuidTime != 0) {
            return uuidTime;
        }
        return Long.compareUnsigned(a.id.getLeastSignificantBits(), b.id.getLeastSignificantBits());
    }

    boolean isAfter(EventView event, boolean ascending) {
        int cmp = compare(of(event), this);
        return ascending ? cmp > 0 : cmp < 0;
    }

    static List<EventView> page(List<EventView> events, boolean ascending, int limit) {
        Comparator<EventView> order = (a, b) -> compare(of(a), of(b));
        return events.stream().sorted(ascending ? order : order.reversed()).limit(limit).toList();
    }

    String encode() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                (at.toEpochMilli() + "/" + id).getBytes(StandardCharsets.UTF_8));
    }

    static StreamCursor decode(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            String[] parts = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8).split("/");
            if (parts.length != 2) {
                throw new IllegalArgumentException();
            }
            UUID id = UUID.fromString(parts[1]);
            if (id.version() != 1) {
                throw new IllegalArgumentException();
            }
            return new StreamCursor(Instant.ofEpochMilli(Long.parseLong(parts[0])), id);
        } catch (RuntimeException e) {
            throw new ValidationException("Некорректный pageToken");
        }
    }
}
