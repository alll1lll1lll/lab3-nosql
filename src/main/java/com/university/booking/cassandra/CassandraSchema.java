package com.university.booking.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.time.Duration;
import java.util.List;

public final class CassandraSchema {

    private static final Duration DDL_TIMEOUT = Duration.ofSeconds(60);

    private CassandraSchema() {
    }

    public static void create(CqlSession session, String keyspace, String datacenter, int replicationFactor) {
        execute(session, keyspace(keyspace, datacenter, replicationFactor));
        tables(keyspace).forEach(ddl -> execute(session, ddl));
    }

    public static void drop(CqlSession session, String keyspace) {
        execute(session, "DROP KEYSPACE IF EXISTS " + keyspace);
    }

    private static void execute(CqlSession session, String cql) {
        session.execute(SimpleStatement.newInstance(cql).setTimeout(DDL_TIMEOUT));
    }

    private static String keyspace(String keyspace, String datacenter, int replicationFactor) {
        return """
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {'class': 'NetworkTopologyStrategy', '%s': %d}
                AND durable_writes = true""".formatted(keyspace, datacenter, replicationFactor);
    }

    private static List<String> tables(String ks) {
        return List.of(
                """
                CREATE TABLE IF NOT EXISTS %s.events_by_type (
                    event_type  text,
                    bucket      int,
                    occurred_at timestamp,
                    event_id    timeuuid,
                    seq         bigint,
                    entity_type text,
                    entity_id   text,
                    actor_id    text,
                    summary     text,
                    payload     text,
                    PRIMARY KEY ((event_type, bucket), occurred_at, event_id)
                ) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id DESC)
                  AND comment = 'Q1: events of a given type for a period; bucket = 100000 events'""".formatted(ks),

                """
                CREATE TABLE IF NOT EXISTS %s.events_by_entity (
                    entity_type text,
                    entity_id   text,
                    occurred_at timestamp,
                    event_id    timeuuid,
                    event_type  text,
                    actor_id    text,
                    summary     text,
                    payload     text,
                    PRIMARY KEY ((entity_type, entity_id), occurred_at, event_id)
                ) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id DESC)
                  AND comment = 'Q2: change history of one object'""".formatted(ks),

                """
                CREATE TABLE IF NOT EXISTS %s.events_by_actor (
                    actor_id    text,
                    bucket      int,
                    occurred_at timestamp,
                    event_id    timeuuid,
                    event_type  text,
                    entity_type text,
                    entity_id   text,
                    summary     text,
                    payload     text,
                    PRIMARY KEY ((actor_id, bucket), occurred_at, event_id)
                ) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id DESC)
                  AND comment = 'Q3: actions of one person for a period; bucket = 100000 events'""".formatted(ks),

                """
                CREATE TABLE IF NOT EXISTS %s.event_buckets (
                    stream         text,
                    bucket         int,
                    first_event_at timestamp,
                    PRIMARY KEY ((stream), bucket)
                ) WITH CLUSTERING ORDER BY (bucket DESC)
                  AND comment = 'bucket catalog per stream'""".formatted(ks),

                """
                CREATE TABLE IF NOT EXISTS %s.events_by_type_unbucketed (
                    event_type  text,
                    occurred_at timestamp,
                    event_id    timeuuid,
                    seq         bigint,
                    entity_type text,
                    entity_id   text,
                    actor_id    text,
                    summary     text,
                    payload     text,
                    PRIMARY KEY ((event_type), occurred_at, event_id)
                ) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id DESC)
                  AND comment = 'experiment: Q1 without bucketing (hot/large partition)'""".formatted(ks));
    }
}
