package com.university.booking.service.organization;

import com.university.booking.dto.organization.OrganizationRequest;
import com.university.booking.enums.BookingStatus;
import com.university.booking.events.EventDraft;
import com.university.booking.events.EventPayload;
import com.university.booking.events.EventRecorder;
import com.university.booking.events.EventType;
import com.university.booking.exception.ConflictException;
import com.university.booking.exception.ResourceNotFoundException;
import com.university.booking.exception.ValidationException;
import com.university.booking.model.Organization;
import com.university.booking.model.OrganizationStats;
import com.university.booking.repository.BookingRepository;
import com.university.booking.repository.OrganizationRepository;
import com.university.booking.service.analytics.AnalyticsCache;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrganizationService {

    private final OrganizationRepository organizationRepository;
    private final BookingRepository bookingRepository;
    private final MongoTemplate mongoTemplate;
    private final AnalyticsCache analyticsCache;
    private final EventRecorder events;

    public List<Organization> getAll() {
        return organizationRepository.findAllByOrderByNameAsc();
    }

    public Organization get(String id) {
        return organizationRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(id));
    }

    public Organization create(OrganizationRequest request) {
        String id = request.getId() != null && !request.getId().isBlank()
                ? request.getId()
                : UUID.randomUUID().toString();
        if (organizationRepository.existsById(id)) {
            throw new ConflictException("Организация с идентификатором %s уже существует".formatted(id));
        }

        Organization organization = Organization.builder()
                .id(id)
                .name(request.getName())
                .type(request.getType())
                .contact(request.getContact())
                .active(request.getActive() == null || request.getActive())
                .stats(new OrganizationStats())
                .createdAt(LocalDateTime.now())
                .build();

        Organization saved = organizationRepository.insert(organization);
        analyticsCache.invalidateAfterCommit();
        events.record(EventDraft.of(EventType.ORGANIZATION_CREATED, saved.getId(), EventPayload.of(
                "name", saved.getName(), "type", saved.getType(), "active", saved.isActive())));
        return saved;
    }

    public Organization update(String id, OrganizationRequest request) {
        Organization organization = get(id);
        Map<String, Object> before = EventPayload.of("name", organization.getName(),
                "type", organization.getType(), "contact", organization.getContact(), "active", organization.isActive());
        organization.setName(request.getName());
        organization.setType(request.getType());
        organization.setContact(request.getContact());
        if (request.getActive() != null) {
            organization.setActive(request.getActive());
        }
        Organization saved = organizationRepository.save(organization);
        analyticsCache.invalidateAfterCommit();
        events.record(EventDraft.of(EventType.ORGANIZATION_UPDATED, id, EventPayload.of(
                "before", before,
                "after", EventPayload.of("name", saved.getName(), "type", saved.getType(),
                        "contact", saved.getContact(), "active", saved.isActive()))));
        return saved;
    }

    public void delete(String id) {
        Organization deleted = get(id);
        if (bookingRepository.existsByOrganizationId(id)) {
            throw new ConflictException("У организации есть заявки: удалить её нельзя, можно только деактивировать");
        }
        organizationRepository.deleteById(id);
        analyticsCache.invalidateAfterCommit();
        events.record(EventDraft.of(EventType.ORGANIZATION_DELETED, id,
                EventPayload.of("name", deleted.getName())));
    }

    public void requireActive(String id) {
        Organization organization = get(id);
        if (!organization.isActive()) {
            throw new ValidationException("Организация %s неактивна, заявки от её имени не принимаются".formatted(id));
        }
    }

    public void moveStats(String organizationId, BookingStatus from, BookingStatus to) {
        if (organizationId == null || from == to) {
            return;
        }
        Update update = new Update();
        if (from != null) {
            update.inc("stats." + from.getStatsField(), -1);
        } else {
            update.inc("stats.total", 1);
        }
        if (to != null) {
            update.inc("stats." + to.getStatsField(), 1);
        } else {
            update.inc("stats.total", -1);
        }
        mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(organizationId)), update, Organization.class);
    }

    @Transactional
    public void recalculateStats() {
        Map<String, OrganizationStats> stats = new HashMap<>();
        List<Document> pipeline = List.of(
                new Document("$match", new Document("organizationId", new Document("$ne", null))),
                new Document("$group", new Document("_id",
                        new Document("org", "$organizationId").append("status", "$status"))
                        .append("count", new Document("$sum", 1))));

        for (Document row : mongoTemplate.getCollection("bookings").aggregate(pipeline)) {
            Document key = row.get("_id", Document.class);
            OrganizationStats s = stats.computeIfAbsent(key.getString("org"), k -> new OrganizationStats());
            long count = ((Number) row.get("count")).longValue();
            s.setTotal(s.getTotal() + count);
            switch (BookingStatus.valueOf(key.getString("status"))) {
                case DRAFT -> s.setDraft(count);
                case UNDER_REVIEW -> s.setUnderReview(count);
                case APPROVED -> s.setApproved(count);
                case REJECTED -> s.setRejected(count);
            }
        }

        for (Organization organization : organizationRepository.findAll()) {
            OrganizationStats s = stats.getOrDefault(organization.getId(), new OrganizationStats());
            mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(organization.getId())),
                    Update.update("stats", s), Organization.class);
        }
        analyticsCache.invalidateAfterCommit();
    }
}