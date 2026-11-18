package com.university.booking.events;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.dto.cassandra.EventWriterStats;
import com.university.booking.web.PersonAuthInterceptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventRecorder {

    private static final int MAX_PENDING = 50_000;
    private static final int RETRY_BATCH_SIZE = 500;
    private static final long RETRY_INTERVAL_SECONDS = 5;
    private static final String SYSTEM_ACTOR = "system";

    private final EventStore eventStore;
    private final BucketAllocator buckets;
    private final CassandraConnector cassandra;
    private final ObjectMapper objectMapper;

    private final BlockingQueue<StoredEvent> pending = new LinkedBlockingQueue<>(MAX_PENDING);
    private final BlockingQueue<Deferred> deferred = new LinkedBlockingQueue<>(MAX_PENDING);
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong failedAttempts = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    public void record(EventDraft draft) {
        String actor = draft.actorId() != null ? draft.actorId() : currentPersonId();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    submit(new Deferred(Uuids.timeBased(), draft, actor));
                }
            });
        } else {
            submit(new Deferred(Uuids.timeBased(), draft, actor));
        }
    }

    public StoredEvent prepare(EventType type, String entityId, String actor, String summary, String payloadJson) {
        return prepare(Uuids.timeBased(), type, entityId, actor, summary, payloadJson);
    }

    private StoredEvent prepare(UUID eventId, EventType type, String entityId, String actor, String summary,
                                String payloadJson) {
        String actorId = actor != null ? actor : SYSTEM_ACTOR;
        return StoredEvent.create(eventId, type, entityId, actorId, summary, payloadJson,
                buckets.allocate(EventStream.TYPE.key(type.name())),
                buckets.allocate(EventStream.ACTOR.key(actorId)));
    }

    public EventWriterStats stats() {
        return new EventWriterStats(written.get(), pending.size() + deferred.size(), failedAttempts.get(), dropped.get());
    }

    @Scheduled(fixedDelay = RETRY_INTERVAL_SECONDS, timeUnit = TimeUnit.SECONDS)
    public void retryPending() {
        List<Deferred> drafts = new ArrayList<>();
        deferred.drainTo(drafts, RETRY_BATCH_SIZE);
        drafts.forEach(this::submit);
        if (pending.isEmpty() || !cassandra.isConnected()) {
            return;
        }
        List<StoredEvent> batch = new ArrayList<>();
        pending.drainTo(batch, RETRY_BATCH_SIZE);
        log.atInfo().addKeyValue("event", "event_retry").addKeyValue("count", batch.size()).log("retrying pending events");
        batch.forEach(this::send);
    }

    private void submit(Deferred item) {
        EventDraft draft = item.draft();
        StoredEvent event;
        try {
            event = prepare(item.eventId(), draft.type(), draft.entityId(), item.actor(), draft.summary(),
                    toJson(draft.payload()));
        } catch (RuntimeException e) {
            failedAttempts.incrementAndGet();
            if (!deferred.offer(item)) {
                dropped.incrementAndGet();
                log.atError().addKeyValue("event", "event_dropped").setCause(e)
                        .log("deferred queue is full, event dropped");
            }
            return;
        }
        if (cassandra.isConnected()) {
            send(event);
        } else {
            enqueue(event);
        }
    }

    private void send(StoredEvent event) {
        try {
            eventStore.writeAsync(event, null).whenComplete((rs, error) -> {
                if (error == null) {
                    written.incrementAndGet();
                } else {
                    onWriteFailed(event, error);
                }
            });
        } catch (RuntimeException e) {
            onWriteFailed(event, e);
        }
    }

    private void onWriteFailed(StoredEvent event, Throwable error) {
        failedAttempts.incrementAndGet();
        log.atWarn().addKeyValue("event", "event_write_failed")
                .addKeyValue("eventType", event.eventType())
                .addKeyValue("error", error.getClass().getSimpleName())
                .log("event write failed, queued for retry: {}", error.getMessage());
        enqueue(event);
    }

    private void enqueue(StoredEvent event) {
        if (!pending.offer(event)) {
            dropped.incrementAndGet();
            log.atError().addKeyValue("event", "event_dropped").log("retry queue is full, event dropped");
        }
    }

    private String toJson(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return String.valueOf(payload);
        }
    }

    private static String currentPersonId() {
        RequestAttributes attrs = RequestContextHolder.getRequestAttributes();
        if (attrs == null) {
            return null;
        }
        Object id = attrs.getAttribute(PersonAuthInterceptor.PERSON_ID_ATTRIBUTE, RequestAttributes.SCOPE_REQUEST);
        return id != null ? id.toString() : null;
    }

    private record Deferred(UUID eventId, EventDraft draft, String actor) {
    }
}
