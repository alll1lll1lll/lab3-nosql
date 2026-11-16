package com.university.booking.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public record CassandraStatements(
        PreparedStatement insertByType,
        PreparedStatement insertByEntity,
        PreparedStatement insertByActor,
        PreparedStatement insertBucket,
        PreparedStatement insertUnbucketed,
        PreparedStatement selectBuckets,
        PreparedStatement selectLastBucket,
        PreparedStatement byTypeDesc,
        PreparedStatement byTypeAsc,
        PreparedStatement byActorDesc,
        PreparedStatement byActorAsc,
        PreparedStatement byEntityDesc,
        PreparedStatement byEntityAsc,
        PreparedStatement byTypePoint,
        PreparedStatement unbucketedDesc,
        PreparedStatement unbucketedAsc,
        PreparedStatement keysByTypeBucket,
        PreparedStatement keysUnbucketed,
        PreparedStatement deleteByTypeBucket,
        PreparedStatement deleteUnbucketed) {

    private static final String RANGE = " AND occurred_at >= ? AND occurred_at <= ?";
    private static final String DESC = " ORDER BY occurred_at DESC, event_id DESC";
    private static final String ASC = " ORDER BY occurred_at ASC, event_id ASC";

    public static CassandraStatements prepare(CqlSession s, String ks) {
        String byType = "SELECT * FROM " + ks + ".events_by_type WHERE event_type = ? AND bucket = ?" + RANGE;
        String byActor = "SELECT * FROM " + ks + ".events_by_actor WHERE actor_id = ? AND bucket = ?" + RANGE;
        String byEntity = "SELECT * FROM " + ks + ".events_by_entity WHERE entity_type = ? AND entity_id = ?" + RANGE;
        String unbucketed = "SELECT * FROM " + ks + ".events_by_type_unbucketed WHERE event_type = ?" + RANGE;
        return new CassandraStatements(
                s.prepare("INSERT INTO " + ks + ".events_by_type (event_type, bucket, occurred_at, event_id, seq,"
                        + " entity_type, entity_id, actor_id, summary, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                s.prepare("INSERT INTO " + ks + ".events_by_entity (entity_type, entity_id, occurred_at, event_id,"
                        + " event_type, actor_id, summary, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"),
                s.prepare("INSERT INTO " + ks + ".events_by_actor (actor_id, bucket, occurred_at, event_id,"
                        + " event_type, entity_type, entity_id, summary, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                s.prepare("INSERT INTO " + ks + ".event_buckets (stream, bucket, first_event_at) VALUES (?, ?, ?)"
                        + " USING TIMESTAMP ?"),
                s.prepare("INSERT INTO " + ks + ".events_by_type_unbucketed (event_type, occurred_at, event_id, seq,"
                        + " entity_type, entity_id, actor_id, summary, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                s.prepare("SELECT bucket, first_event_at FROM " + ks + ".event_buckets WHERE stream = ?"),
                s.prepare("SELECT bucket FROM " + ks + ".event_buckets WHERE stream = ? LIMIT 1"),
                s.prepare(byType + DESC),
                s.prepare(byType + ASC),
                s.prepare(byActor + DESC),
                s.prepare(byActor + ASC),
                s.prepare(byEntity + DESC),
                s.prepare(byEntity + ASC),
                s.prepare("SELECT event_id FROM " + ks + ".events_by_type WHERE event_type = ? AND bucket = ?"
                        + " AND occurred_at = ? AND event_id = ?"),
                s.prepare(unbucketed + DESC),
                s.prepare(unbucketed + ASC),
                s.prepare("SELECT event_id FROM " + ks + ".events_by_type WHERE event_type = ? AND bucket = ?"),
                s.prepare("SELECT event_id FROM " + ks + ".events_by_type_unbucketed WHERE event_type = ?"),
                s.prepare("DELETE FROM " + ks + ".events_by_type WHERE event_type = ? AND bucket = ?"),
                s.prepare("DELETE FROM " + ks + ".events_by_type_unbucketed WHERE event_type = ?"));
    }
}
