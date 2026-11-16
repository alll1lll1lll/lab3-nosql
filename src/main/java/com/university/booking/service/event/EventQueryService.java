package com.university.booking.service.event;

import com.university.booking.cassandra.ConsistencyLevels;
import com.university.booking.dto.event.EventPage;
import com.university.booking.dto.event.EventQueryParams;
import com.university.booking.dto.event.EventTypeView;
import com.university.booking.events.EntityType;
import com.university.booking.events.EventQuery;
import com.university.booking.events.EventStore;
import com.university.booking.events.EventStream;
import com.university.booking.events.EventType;
import com.university.booking.exception.AccessDeniedException;
import com.university.booking.exception.ValidationException;
import com.university.booking.service.booking.BookingService;
import com.university.booking.service.security.AccessService;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventQueryService {

    private static final Instant MIN_TIME = Instant.EPOCH;
    private static final Instant MAX_TIME = Instant.parse("9999-12-31T23:59:59Z");

    private final EventStore eventStore;
    private final AccessService accessService;
    private final BookingService bookingService;

    public List<EventTypeView> types() {
        return Arrays.stream(EventType.values())
                .map(t -> new EventTypeView(t.name(), t.getEntityType().name(), t.getTitle()))
                .toList();
    }

    public EventPage byType(String type, EventQueryParams params) {
        return eventStore.queryStream(EventStream.TYPE, parseEnum(EventType.class, type,
                "Неизвестный тип события: " + type + ". Список: GET /api/events/types").name(), toQuery(params));
    }

    public EventPage byEntity(String personId, String entityType, String entityId, EventQueryParams params) {
        EntityType entity = parseEnum(EntityType.class, entityType, "Неизвестный тип объекта: " + entityType);
        requireEntityAccess(personId, entity, entityId);
        return eventStore.queryEntity(entity.name(), entityId, toQuery(params));
    }

    public EventPage byActor(String personId, String actorId, EventQueryParams params) {
        if (!actorId.equals(personId) && !accessService.isAdmin(personId)) {
            throw new AccessDeniedException("Можно просматривать только свои действия");
        }
        return eventStore.queryStream(EventStream.ACTOR, actorId, toQuery(params));
    }

    private void requireEntityAccess(String personId, EntityType entity, String entityId) {
        if (accessService.isAdmin(personId)) {
            return;
        }
        switch (entity) {
            case BOOKING -> bookingService.getBooking(entityId, personId);
            case PERSON -> {
                if (!entityId.equals(personId)) {
                    throw new AccessDeniedException("Можно просматривать только историю своего профиля");
                }
            }
            default -> throw new AccessDeniedException("История этого объекта доступна только администратору");
        }
    }

    private static EventQuery toQuery(EventQueryParams params) {
        Instant from = parseTime(params.getFrom(), false, MIN_TIME);
        Instant to = parseTime(params.getTo(), true, MAX_TIME);
        if (from.isAfter(to)) {
            throw new ValidationException("Начало периода позже конца");
        }
        return new EventQuery(from, to, ascending(params.getOrder()), params.getSize(), params.getPageToken(),
                ConsistencyLevels.parseOrDefault(params.getConsistency()));
    }

    private static Instant parseTime(String value, boolean endOfRange, Instant fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        String v = value.trim();
        ZoneId zone = ZoneId.systemDefault();
        try {
            if (v.length() == 10) {
                LocalDate date = LocalDate.parse(v);
                return endOfRange
                        ? date.plusDays(1).atStartOfDay(zone).toInstant().minusMillis(1)
                        : date.atStartOfDay(zone).toInstant();
            }
            return hasOffset(v) ? OffsetDateTime.parse(v).toInstant() : LocalDateTime.parse(v).atZone(zone).toInstant();
        } catch (DateTimeParseException e) {
            throw new ValidationException("Некорректная дата/время: " + value
                    + " (ожидается 2026-11-08, 2026-11-08T10:00 или 2026-11-08T10:00:00+03:00)");
        }
    }

    private static boolean hasOffset(String value) {
        int timeStart = value.indexOf('T');
        return value.endsWith("Z") || timeStart > 0
                && (value.indexOf('+', timeStart) > 0 || value.indexOf('-', timeStart) > 0);
    }

    private static boolean ascending(String order) {
        return switch (order.trim().toLowerCase()) {
            case "asc" -> true;
            case "desc" -> false;
            default -> throw new ValidationException("order должен быть asc или desc");
        };
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String value, String error) {
        try {
            return Enum.valueOf(type, value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ValidationException(error);
        }
    }
}
