package com.university.booking.experiments;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.cassandra.CassandraSchema;
import com.university.booking.cassandra.CassandraStatements;
import com.university.booking.cassandra.ClusterTopology;
import com.university.booking.config.CassandraProperties;
import com.university.booking.dto.cassandra.HotPartitionFillReport;
import com.university.booking.dto.cassandra.HotPartitionReport;
import com.university.booking.dto.cassandra.HotPartitionResetReport;
import com.university.booking.dto.cassandra.LatencyStats;
import com.university.booking.dto.cassandra.PartitionReport;
import com.university.booking.dto.cassandra.ReadComparison;
import com.university.booking.dto.cassandra.ReadLatencyReport;
import com.university.booking.dto.cassandra.Rf1Distribution;
import com.university.booking.events.BucketAllocation;
import com.university.booking.events.BucketAllocator;
import com.university.booking.events.BucketInfo;
import com.university.booking.events.EventStore;
import com.university.booking.events.EventStream;
import com.university.booking.events.EventType;
import com.university.booking.events.StoredEvent;
import com.university.booking.exception.ValidationException;
import com.university.booking.util.Timings;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HotPartitionExperimentService {

    public static final String RF1_KEYSPACE = "hot_partition_lab";

    private static final int MAX_EVENTS = 2_000_000;
    private static final int MAX_CONCURRENCY = 512;
    private static final int MAX_REPEATS = 200;
    private static final int READ_PAGE_SIZE = 100;
    private static final int COUNT_PAGE_SIZE = 5000;
    private static final int PROJECTED_BUCKETS = 30;
    private static final int ENTITY_SPREAD = 1000;
    private static final Duration LONG_TIMEOUT = Duration.ofSeconds(60);
    private static final Instant MAX_TIME = Instant.parse("9999-12-31T00:00:00Z");
    private static final String FILLER = "x".repeat(160);
    private static final String TYPE = EventType.HOT_PARTITION_DEMO.name();
    private static final String STREAM = EventStream.TYPE.key(TYPE);

    private final CassandraConnector cassandra;
    private final EventStore eventStore;
    private final BucketAllocator buckets;

    public HotPartitionFillReport fill(int count, int concurrency) {
        int n = Math.clamp(count, 1, MAX_EVENTS);
        int parallelism = Math.clamp(concurrency, 1, MAX_CONCURRENCY);
        CassandraConnector.Connection c = cassandra.require();
        CassandraStatements rf1 = rf1Statements(c.session());
        long start = buckets.reserveOrFail(STREAM, n);
        BucketAllocation noActor = BucketAllocation.withoutSeq(EventStream.ACTOR.key(ExperimentActors.EXPERIMENT), 0, false);

        String ks = cassandra.properties().keyspace();
        Map<String, LatencySeries> series = new LinkedHashMap<>();
        LatencySeries bucketedMain = series.computeIfAbsent(ks + ".events_by_type", k -> new LatencySeries(n));
        LatencySeries unbucketedMain = series.computeIfAbsent(ks + ".events_by_type_unbucketed", k -> new LatencySeries(n));
        LatencySeries bucketedRf1 = series.computeIfAbsent(RF1_KEYSPACE + ".events_by_type", k -> new LatencySeries(n));
        LatencySeries unbucketedRf1 = series.computeIfAbsent(RF1_KEYSPACE + ".events_by_type_unbucketed",
                k -> new LatencySeries(n));

        long started = System.nanoTime();
        ConcurrentRunner.run(IntStream.range(0, n).boxed().flatMap(i -> {
            long seq = start + i;
            BucketAllocation allocation = buckets.toAllocation(STREAM, seq);
            StoredEvent e = StoredEvent.create(EventType.HOT_PARTITION_DEMO, "hot-" + (seq % ENTITY_SPREAD),
                    ExperimentActors.EXPERIMENT, "hot partition demo " + seq,
                    "{\"seq\":" + seq + ",\"filler\":\"" + FILLER + "\"}", allocation, noActor);
            return Stream.of(
                    new ConcurrentRunner.Op(bucketedMain, () -> eventStore.writeTypeOnlyAsync(e, null)),
                    new ConcurrentRunner.Op(unbucketedMain, () -> c.session().executeAsync(
                            bindUnbucketed(c.statements(), e))),
                    new ConcurrentRunner.Op(bucketedRf1, () -> c.session().executeAsync(bindBucketed(rf1, e))),
                    new ConcurrentRunner.Op(unbucketedRf1, () -> c.session().executeAsync(bindUnbucketed(rf1, e))));
        }).iterator(), parallelism);
        double seconds = Timings.secondsSince(started);

        Map<String, LatencyStats> writes = new LinkedHashMap<>();
        series.forEach((table, s) -> writes.put(table, s.stats()));
        return new HotPartitionFillReport(n, List.of(start, start + n - 1),
                List.of(buckets.bucketOf(start), buckets.bucketOf(start + n - 1)),
                Timings.round(seconds), Timings.round(n * series.size() / seconds), writes);
    }

    public HotPartitionReport report(int repeats) {
        CassandraConnector.Connection c = cassandra.require();
        CqlSession s = c.session();
        CassandraStatements main = c.statements();
        CassandraStatements rf1 = rf1Statements(s);
        CassandraProperties props = cassandra.properties();
        List<PartitionReport> bucketed = new ArrayList<>();
        List<BucketInfo> filled = new ArrayList<>();
        Map<String, Long> rf1Bucketed = new TreeMap<>();
        for (BucketInfo b : eventStore.buckets(STREAM)) {
            PartitionReport report = partition("(" + TYPE + ", " + b.bucket() + ")", s, props.keyspace(),
                    main.keysByTypeBucket().bind(TYPE, b.bucket()), rf1.keysByTypeBucket().bind(TYPE, b.bucket()),
                    rf1Bucketed);
            if (report.rows() > 0) {
                bucketed.add(report);
                filled.add(b);
            }
        }
        if (filled.isEmpty()) {
            throw new ValidationException("Нет данных: сначала POST /api/admin/cassandra/experiments/hot-partition/fill");
        }
        Map<String, Long> rf1Unbucketed = new TreeMap<>();
        PartitionReport unbucketed = partition("(" + TYPE + ")", s, props.keyspace(),
                main.keysUnbucketed().bind(TYPE), rf1.keysUnbucketed().bind(TYPE), rf1Unbucketed);

        return new HotPartitionReport(props.bucketSize(), bucketed, unbucketed,
                new Rf1Distribution(rf1Bucketed, rf1Unbucketed),
                projectedRf1Owners(s, main),
                readLatency(s, main, filled, Math.clamp(repeats, 1, MAX_REPEATS)));
    }

    public HotPartitionResetReport reset() {
        CassandraConnector.Connection c = cassandra.require();
        CassandraStatements st = c.statements();
        List<BucketInfo> catalog = eventStore.buckets(STREAM);
        catalog.forEach(b -> c.session().execute(st.deleteByTypeBucket().bind(TYPE, b.bucket())));
        c.session().execute(st.deleteUnbucketed().bind(TYPE));
        CassandraSchema.drop(c.session(), RF1_KEYSPACE);
        return new HotPartitionResetReport(catalog.size(), RF1_KEYSPACE);
    }

    private CassandraStatements rf1Statements(CqlSession session) {
        CassandraSchema.create(session, RF1_KEYSPACE, cassandra.properties().localDatacenter(), 1);
        return CassandraStatements.prepare(session, RF1_KEYSPACE);
    }

    private static PartitionReport partition(String name, CqlSession s, String keyspace, BoundStatement mainKeys,
                                             BoundStatement rf1Keys, Map<String, Long> rf1RowsPerNode) {
        long started = System.nanoTime();
        long rows = countRows(s, mainKeys);
        double countMs = Timings.millisSince(started);
        List<String> rf1Owner = ClusterTopology.replicas(s, RF1_KEYSPACE, rf1Keys);
        long rf1Rows = countRows(s, rf1Keys);
        if (rf1Rows > 0) {
            rf1Owner.forEach(node -> rf1RowsPerNode.merge(node, rf1Rows, Long::sum));
        }
        return new PartitionReport(name, rows, countMs, ClusterTopology.replicas(s, keyspace, mainKeys), rf1Owner);
    }

    private static Map<String, Integer> projectedRf1Owners(CqlSession s, CassandraStatements st) {
        Map<String, Integer> owners = new TreeMap<>();
        for (int bucket = 0; bucket < PROJECTED_BUCKETS; bucket++) {
            ClusterTopology.replicas(s, RF1_KEYSPACE, st.keysByTypeBucket().bind(TYPE, bucket))
                    .forEach(node -> owners.merge(node, 1, Integer::sum));
        }
        return owners;
    }

    private static ReadLatencyReport readLatency(CqlSession s, CassandraStatements st, List<BucketInfo> filled,
                                                 int repeats) {
        Instant first = filled.getFirst().firstEventAt();
        Instant middle = first.plusMillis((Instant.now().toEpochMilli() - first.toEpochMilli()) / 2);
        int firstBucket = filled.getFirst().bucket();
        int middleBucket = filled.get(filled.size() / 2).bucket();
        int lastBucket = filled.getLast().bucket();
        return new ReadLatencyReport(
                new ReadComparison(
                        timeReads(s, st.byTypeDesc().bind(TYPE, lastBucket, Instant.EPOCH, MAX_TIME), repeats),
                        timeReads(s, st.unbucketedDesc().bind(TYPE, Instant.EPOCH, MAX_TIME), repeats)),
                new ReadComparison(
                        timeReads(s, st.byTypeAsc().bind(TYPE, firstBucket, Instant.EPOCH, MAX_TIME), repeats),
                        timeReads(s, st.unbucketedAsc().bind(TYPE, Instant.EPOCH, MAX_TIME), repeats)),
                new ReadComparison(
                        timeReads(s, st.byTypeAsc().bind(TYPE, middleBucket, middle, MAX_TIME), repeats),
                        timeReads(s, st.unbucketedAsc().bind(TYPE, middle, MAX_TIME), repeats)));
    }

    private static long countRows(CqlSession s, BoundStatement keys) {
        long rows = 0;
        for (Row ignored : s.execute(keys
                .setConsistencyLevel(DefaultConsistencyLevel.ONE)
                .setPageSize(COUNT_PAGE_SIZE)
                .setTimeout(LONG_TIMEOUT))) {
            rows++;
        }
        return rows;
    }

    private static LatencyStats timeReads(CqlSession s, BoundStatement stmt, int repeats) {
        LatencySeries series = new LatencySeries(repeats);
        BoundStatement st = stmt.setPageSize(READ_PAGE_SIZE).setConsistencyLevel(DefaultConsistencyLevel.ONE);
        for (int i = 0; i < repeats; i++) {
            long started = System.nanoTime();
            try {
                s.execute(st);
                series.ok(System.nanoTime() - started);
            } catch (RuntimeException e) {
                series.fail(e, System.nanoTime() - started);
            }
        }
        return series.stats();
    }

    private static BoundStatement bindBucketed(CassandraStatements st, StoredEvent e) {
        return st.insertByType().bind(e.eventType(), e.typeBucket().bucket(), e.occurredAt(), e.eventId(),
                e.typeSeq(), e.entityType(), e.entityId(), e.actorId(), e.summary(), e.payload()).setIdempotent(true);
    }

    private static BoundStatement bindUnbucketed(CassandraStatements st, StoredEvent e) {
        return st.insertUnbucketed().bind(e.eventType(), e.occurredAt(), e.eventId(), e.typeSeq(),
                e.entityType(), e.entityId(), e.actorId(), e.summary(), e.payload()).setIdempotent(true);
    }
}
