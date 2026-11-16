package com.university.booking.dto.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventView(
        UUID eventId,
        OffsetDateTime occurredAt,
        String eventType,
        String entityType,
        String entityId,
        String actorId,
        String summary,
        JsonNode payload,
        Integer bucket,
        Long seq) {
}
