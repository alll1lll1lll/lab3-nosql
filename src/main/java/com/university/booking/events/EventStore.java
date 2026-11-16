package com.university.booking.events;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.cassandra.CassandraStatements;
import com.university.booking.dto.event.EventPage;
import com.university.booking.dto.event.EventView;
import com.university.booking.exception.ValidationException;
import com.university.booking.util.Timings;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventStore {

    private static final int MAX_PAGE_SIZE = 500;

    private final CassandraConnector cassandra;
    private final ObjectMapper objectMapper;

    public CompletionStage<AsyncResultSet> writeAsync(StoredEvent e, ConsistencyLevel consistency) {
        CassandraConnector.Connection c = cassandra.require();
        CassandraStatements st = c.statements();
        BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED)
                .addStatement(bindByType(st, e))
                .addStatement(st.insertByEntity().bind(e.entityType(), e.entityId(), e.occurredAt(), e.eventId(),
                        e.eventType(), e.actorId(), e.summary(), e.payload()))
                .addStatement(st.insertByActor().bind(e.actorId(), e.actorBucket().bucket(), e.occurredAt(), e.eventId(),
                        e.eventType(), e.entityType(), e.entityId(), e.summary(), e.payload()));
        if (e.typeBucket().first()) {
            batch.addStatement(bindCatalog(st, e.typeBucket(), e.occurredAt()));
        }
        if (e.actorBucket().first()) {
            batch.addStatement(bindCatalog(st, e.actorBucket(), e.occurredAt()));
        }
        return c.session().executeAsync(withConsistency(batch.setIdempotence(true).build(), consistency));
    }

    public CompletionStage<AsyncResultSet> writeTypeOnlyAsync(StoredEvent e, ConsistencyLevel consistency) {
        CassandraConnector.Connection c = cassandra.require();
        CassandraStatements st = c.statements();
        BoundStatement row = bindByType(st, e).setIdempotent(true);
        if (!e.typeBucket().first()) {
            return c.session().executeAsync(withConsistency(row, consistency));
        }
        BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatement(row)
                .addStatement(bindCatalog(st, e.typeBucket(), e.occurredAt()))
                .setIdempotence(true)
                .build();
        return c.session().executeAsync(withConsistency(batch, consistency));
    }

    public List<BucketInfo> buckets(String stream) {
        return buckets(stream, null);
    }

    private List<BucketInfo> buckets(String stream, ConsistencyLevel consistency) {
        CassandraConnector.Connection c = cassandra.require();
        List<BucketInfo> result = new ArrayList<>();
        for (Row row : c.session().execute(withConsistency(c.statements().selectBuckets().bind(stream), consistency))) {
            result.add(new BucketInfo(row.getInt("bucket"), row.getInstant("first_event_at")));
        }
        result.sort(Comparator.comparingInt(BucketInfo::bucket));
        return result;
    }

    public EventPage queryStream(EventStream stream, String key, EventQuery query) {
        long started = System.nanoTime();
        int limit = Math.clamp(query.size(), 1, MAX_PAGE_SIZE);
        List<Integer> candidates = overlappingBuckets(buckets(stream.key(key), query.consistency()),
                query.from(), query.to());
        if (!query.ascending()) {
            candidates = candidates.reversed();
        }

        PageCursor cursor = PageCursor.decode(query.pageToken());
        int index = 0;
        if (cursor != null) {
            index = candidates.indexOf(cursor.bucket());
            if (index < 0) {
                throw new ValidationException("pageToken не относится к этому запросу");
            }
        }

        CassandraConnector.Connection c = cassandra.require();
        PreparedStatement ps = streamStatement(c.statements(), stream, query.ascending());
        boolean byType = stream == EventStream.TYPE;
        List<EventView> items = new ArrayList<>(limit);
        List<Integer> scanned = new ArrayList<>();
        String next = null;
        String coordinator = null;
        ByteBuffer pagingState = cursor != null ? cursor.pagingState() : null;

        while (index < candidates.size()) {
            int bucket = candidates.get(index);
            BoundStatement stmt = ps.bind(key, bucket, query.from(), query.to())
                    .setPageSize(limit - items.size())
                    .setPagingState(pagingState);
            AsyncResultSet rs = execute(c, withConsistency(stmt, query.consistency()));
            coordinator = coordinator(rs);
            scanned.add(bucket);
            for (Row row : rs.currentPage()) {
                items.add(toView(row, byType ? bucket : null, byType));
            }
            if (rs.hasMorePages()) {
                pagingState = rs.getExecutionInfo().getPagingState();
                if (items.size() >= limit) {
                    next = new PageCursor(bucket, pagingState).encode();
                    break;
                }
                continue;
            }
            pagingState = null;
            index++;
            if (items.size() >= limit) {
                if (index < candidates.size()) {
                    next = new PageCursor(candidates.get(index), null).encode();
                }
                break;
            }
        }
        return new EventPage(items, items.size(), next, scanned, effective(query.consistency()), coordinator,
                Timings.millisSince(started));
    }

    public EventPage queryEntity(String entityType, String entityId, EventQuery query) {
        long started = System.nanoTime();
        int limit = Math.clamp(query.size(), 1, MAX_PAGE_SIZE);
        CassandraConnector.Connection c = cassandra.require();
        PreparedStatement ps = query.ascending() ? c.statements().byEntityAsc() : c.statements().byEntityDesc();
        PageCursor cursor = PageCursor.decode(query.pageToken());
        BoundStatement stmt = ps.bind(entityType, entityId, query.from(), query.to())
                .setPageSize(limit)
                .setPagingState(cursor != null ? cursor.pagingState() : null);
        AsyncResultSet rs = execute(c, withConsistency(stmt, query.consistency()));
        List<EventView> items = new ArrayList<>(limit);
        for (Row row : rs.currentPage()) {
            items.add(toView(row, null, false));
        }
        String next = rs.hasMorePages() ? new PageCursor(0, rs.getExecutionInfo().getPagingState()).encode() : null;
        return new EventPage(items, items.size(), next, null, effective(query.consistency()), coordinator(rs),
                Timings.millisSince(started));
    }

    static List<Integer> overlappingBuckets(List<BucketInfo> buckets, Instant from, Instant to) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < buckets.size(); i++) {
            BucketInfo b = buckets.get(i);
            Instant nextStart = i + 1 < buckets.size() ? buckets.get(i + 1).firstEventAt() : null;
            boolean startsBeforeEnd = !b.firstEventAt().isAfter(to);
            boolean endsAfterStart = nextStart == null || !nextStart.isBefore(from);
            if (startsBeforeEnd && endsAfterStart) {
                result.add(b.bucket());
            }
        }
        return result;
    }

    private static PreparedStatement streamStatement(CassandraStatements st, EventStream stream, boolean ascending) {
        return switch (stream) {
            case TYPE -> ascending ? st.byTypeAsc() : st.byTypeDesc();
            case ACTOR -> ascending ? st.byActorAsc() : st.byActorDesc();
        };
    }

    private static BoundStatement bindByType(CassandraStatements st, StoredEvent e) {
        return st.insertByType().bind(e.eventType(), e.typeBucket().bucket(), e.occurredAt(), e.eventId(),
                e.typeSeq(), e.entityType(), e.entityId(), e.actorId(), e.summary(), e.payload());
    }

    private static BoundStatement bindCatalog(CassandraStatements st, BucketAllocation a, Instant at) {
        long micros = ChronoUnit.MICROS.between(Instant.EPOCH, at);
        return st.insertBucket().bind(a.stream(), a.bucket(), at, Long.MAX_VALUE - micros);
    }

    private static <S extends Statement<S>> S withConsistency(S statement, ConsistencyLevel consistency) {
        return consistency == null ? statement : statement.setConsistencyLevel(consistency);
    }

    private static AsyncResultSet execute(CassandraConnector.Connection c, Statement<?> statement) {
        return c.session().executeAsync(statement).toCompletableFuture().join();
    }

    private static String coordinator(AsyncResultSet rs) {
        return rs.getExecutionInfo().getCoordinator().getEndPoint().toString();
    }

    private EventView toView(Row row, Integer bucket, boolean hasSeq) {
        Instant at = row.getInstant("occurred_at");
        return new EventView(row.getUuid("event_id"),
                at == null ? null : OffsetDateTime.ofInstant(at, ZoneId.systemDefault()),
                optionalString(row, "event_type"),
                optionalString(row, "entity_type"),
                optionalString(row, "entity_id"),
                optionalString(row, "actor_id"),
                row.getString("summary"),
                parse(row.getString("payload")),
                bucket,
                hasSeq && !row.isNull("seq") ? row.getLong("seq") : null);
    }

    private static String optionalString(Row row, String column) {
        return row.getColumnDefinitions().contains(column) ? row.getString(column) : null;
    }

    private JsonNode parse(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            return objectMapper.getNodeFactory().textNode(json);
        }
    }

    private String effective(ConsistencyLevel consistency) {
        return consistency != null ? consistency.name() : cassandra.properties().consistency();
    }
}
