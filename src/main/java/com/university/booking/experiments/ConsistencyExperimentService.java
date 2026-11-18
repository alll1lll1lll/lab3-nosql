package com.university.booking.experiments;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.cassandra.ClusterTopology;
import com.university.booking.cassandra.ConsistencyLevels;
import com.university.booking.dto.cassandra.ConsistencyLevelResult;
import com.university.booking.dto.cassandra.ConsistencyReport;
import com.university.booking.events.EventRecorder;
import com.university.booking.events.EventStore;
import com.university.booking.events.EventType;
import com.university.booking.events.StoredEvent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ConsistencyExperimentService {

    private static final int MAX_OPS = 10_000;

    private final CassandraConnector cassandra;
    private final EventStore eventStore;
    private final EventRecorder recorder;

    public ConsistencyReport run(List<String> levelNames, int ops, String mode) {
        List<ConsistencyLevel> levels = levelNames.stream()
                .filter(name -> !name.isBlank())
                .map(ConsistencyLevels::parse)
                .toList();
        WriteMode writeMode = WriteMode.parse(mode);
        int n = Math.clamp(ops, 1, MAX_OPS);
        CassandraConnector.Connection c = cassandra.require();
        String runId = UUID.randomUUID().toString();

        Map<ConsistencyLevel, Probe> probes = new LinkedHashMap<>();
        levels.forEach(level -> probes.put(level, new Probe(n)));
        for (int i = 0; i < n; i++) {
            probes.forEach((level, probe) -> probe.run(c, writeAndRead(level, writeMode, runId)));
        }

        int rf = cassandra.properties().replicationFactor();
        Map<String, ConsistencyLevelResult> results = new LinkedHashMap<>();
        probes.forEach((level, probe) -> results.put(level.name(), probe.result(level, rf)));
        return new ConsistencyReport(n, writeMode.getDescription(), ClusterTopology.nodeStates(c.session()), results);
    }

    private ProbeStep writeAndRead(ConsistencyLevel level, WriteMode mode, String runId) {
        StoredEvent event = recorder.prepare(EventType.CONSISTENCY_PROBE, runId, ExperimentActors.EXPERIMENT,
                "probe " + level.name(), null);
        return new ProbeStep(event, level, () -> mode == WriteMode.SINGLE
                ? eventStore.writeTypeOnlyAsync(event, level)
                : eventStore.writeAsync(event, level));
    }

    private record ProbeStep(StoredEvent event, ConsistencyLevel level,
                             Supplier<CompletionStage<?>> write) {
    }

    private static final class Probe {
        private final LatencySeries writes;
        private final LatencySeries reads;
        private int readYourWrites;

        Probe(int capacity) {
            this.writes = new LatencySeries(capacity);
            this.reads = new LatencySeries(capacity);
        }

        void run(CassandraConnector.Connection c, ProbeStep step) {
            boolean written = write(step);
            StoredEvent e = step.event();
            BoundStatement read = c.statements().byTypePoint()
                    .bind(e.eventType(), e.typeBucket().bucket(), e.occurredAt(), e.eventId())
                    .setConsistencyLevel(step.level());
            long started = System.nanoTime();
            try {
                Row row = c.session().executeAsync(read).toCompletableFuture().join().one();
                reads.ok(System.nanoTime() - started);
                if (written && row != null) {
                    readYourWrites++;
                }
            } catch (RuntimeException ex) {
                reads.fail(ex, System.nanoTime() - started);
            }
        }

        private boolean write(ProbeStep step) {
            long started = System.nanoTime();
            try {
                step.write().get().toCompletableFuture().join();
                writes.ok(System.nanoTime() - started);
                return true;
            } catch (RuntimeException ex) {
                writes.fail(ex, System.nanoTime() - started);
                return false;
            }
        }

        ConsistencyLevelResult result(ConsistencyLevel level, int replicationFactor) {
            return new ConsistencyLevelResult(ConsistencyLevels.requiredReplicas(level, replicationFactor),
                    writes.stats(), reads.stats(), readYourWrites);
        }
    }
}
