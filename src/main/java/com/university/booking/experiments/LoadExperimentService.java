package com.university.booking.experiments;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.cassandra.ConsistencyLevels;
import com.university.booking.dto.cassandra.LoadReport;
import com.university.booking.events.BucketAllocator;
import com.university.booking.events.BucketInfo;
import com.university.booking.events.EventStore;
import com.university.booking.events.EventStream;
import com.university.booking.events.EventType;
import com.university.booking.events.StoredEvent;
import com.university.booking.exception.ValidationException;
import com.university.booking.util.Timings;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LoadExperimentService {

    private static final int MAX_OPS = 1_000_000;
    private static final int MAX_CONCURRENCY = 1024;
    private static final int READ_PAGE_SIZE = 50;
    private static final int ENTITY_SPREAD = 1000;

    private final CassandraConnector cassandra;
    private final EventStore eventStore;
    private final BucketAllocator buckets;

    public LoadReport run(String kindName, int ops, int concurrency, String consistency, double writeRatio) {
        LoadKind kind = LoadKind.parse(kindName);
        double ratio = kind.writeRatio(writeRatio);
        int n = Math.clamp(ops, 1, MAX_OPS);
        int parallelism = Math.clamp(concurrency, 1, MAX_CONCURRENCY);
        ConsistencyLevel level = ConsistencyLevels.parseOrDefault(consistency);
        LoadPlan plan = plan(n, ratio, level);

        long started = System.nanoTime();
        ConcurrentRunner.run(IntStream.range(0, n).mapToObj(plan::op).iterator(), parallelism);
        double seconds = Timings.secondsSince(started);

        return new LoadReport(kind.name().toLowerCase(), n, parallelism,
                level == null ? cassandra.properties().consistency() : level.name(),
                Timings.round(seconds), Timings.round(n / seconds),
                plan.writes.isEmpty() ? null : plan.writes.stats(),
                plan.reads.isEmpty() ? null : plan.reads.stats());
    }

    private LoadPlan plan(int ops, double ratio, ConsistencyLevel level) {
        String typeStream = EventStream.TYPE.key(EventType.LOAD_TEST.name());
        String actorStream = EventStream.ACTOR.key(ExperimentActors.LOAD_TEST);
        long writesPlanned = Math.round(ops * ratio) + 1;
        long typeStart = ratio > 0 ? buckets.reserveOrFail(typeStream, writesPlanned) : 0;
        long actorStart = ratio > 0 ? buckets.reserveOrFail(actorStream, writesPlanned) : 0;

        List<BucketInfo> existing = eventStore.buckets(typeStream);
        if (ratio < 1.0 && existing.isEmpty()) {
            throw new ValidationException("Нет данных для чтения: сначала запусти kind=write");
        }
        Instant readFrom = existing.isEmpty() ? Instant.now() : existing.getFirst().firstEventAt();
        return new LoadPlan(ops, ratio, level, typeStream, actorStream, typeStart, actorStart, existing, readFrom);
    }

    private final class LoadPlan {
        private final double ratio;
        private final ConsistencyLevel level;
        private final String typeStream;
        private final String actorStream;
        private final long typeStart;
        private final long actorStart;
        private final List<BucketInfo> existing;
        private final Instant readFrom;
        private final LatencySeries writes;
        private final LatencySeries reads;
        private long writesIssued;

        LoadPlan(int ops, double ratio, ConsistencyLevel level, String typeStream, String actorStream,
                 long typeStart, long actorStart, List<BucketInfo> existing, Instant readFrom) {
            this.ratio = ratio;
            this.level = level;
            this.typeStream = typeStream;
            this.actorStream = actorStream;
            this.typeStart = typeStart;
            this.actorStart = actorStart;
            this.existing = existing;
            this.readFrom = readFrom;
            this.writes = new LatencySeries(ops);
            this.reads = new LatencySeries(ops);
        }

        ConcurrentRunner.Op op(int i) {
            boolean isWrite = Math.floor((i + 1) * ratio) > Math.floor(i * ratio);
            return isWrite ? writeOp() : readOp();
        }

        private ConcurrentRunner.Op writeOp() {
            long k = writesIssued++;
            StoredEvent e = StoredEvent.create(EventType.LOAD_TEST, "load-" + (k % ENTITY_SPREAD),
                    ExperimentActors.LOAD_TEST, "load test event", "{\"i\":" + k + "}",
                    buckets.toAllocation(typeStream, typeStart + k), buckets.toAllocation(actorStream, actorStart + k));
            return new ConcurrentRunner.Op(writes, () -> eventStore.writeAsync(e, level));
        }

        private ConcurrentRunner.Op readOp() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            long span = Math.max(1, Instant.now().toEpochMilli() - readFrom.toEpochMilli());
            Instant to = readFrom.plusMillis(random.nextLong(span) + 1);
            int bucket = existing.get(random.nextInt(existing.size())).bucket();
            CassandraConnector.Connection c = cassandra.require();
            BoundStatement stmt = c.statements().byTypeDesc()
                    .bind(EventType.LOAD_TEST.name(), bucket, readFrom, to)
                    .setPageSize(READ_PAGE_SIZE);
            BoundStatement withLevel = level == null ? stmt : stmt.setConsistencyLevel(level);
            return new ConcurrentRunner.Op(reads, () -> c.session().executeAsync(withLevel));
        }
    }
}
