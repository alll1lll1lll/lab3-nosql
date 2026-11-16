package com.university.booking.events;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.util.UUID;

public record StoredEvent(
        UUID eventId,
        Instant occurredAt,
        String eventType,
        String entityType,
        String entityId,
        String actorId,
        String summary,
        String payload,
        BucketAllocation typeBucket,
        BucketAllocation actorBucket) {

    public static StoredEvent create(EventType type, String entityId, String actorId, String summary, String payload,
                                     BucketAllocation typeBucket, BucketAllocation actorBucket) {
        UUID id = Uuids.timeBased();
        Instant at = Instant.ofEpochMilli(Uuids.unixTimestamp(id));
        return new StoredEvent(id, at, type.name(), type.getEntityType().name(), entityId, actorId, summary, payload,
                typeBucket, actorBucket);
    }

    public Long typeSeq() {
        return typeBucket.hasSeq() ? typeBucket.seq() : null;
    }
}
