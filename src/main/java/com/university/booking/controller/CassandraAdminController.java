package com.university.booking.controller;

import com.university.booking.dto.cassandra.ClusterStatus;
import com.university.booking.dto.cassandra.ConsistencyReport;
import com.university.booking.dto.cassandra.HotPartitionFillReport;
import com.university.booking.dto.cassandra.HotPartitionReport;
import com.university.booking.dto.cassandra.HotPartitionResetReport;
import com.university.booking.dto.cassandra.LoadReport;
import com.university.booking.events.BucketInfo;
import com.university.booking.events.EventStore;
import com.university.booking.experiments.ClusterStatusService;
import com.university.booking.experiments.ConsistencyExperimentService;
import com.university.booking.experiments.HotPartitionExperimentService;
import com.university.booking.experiments.LoadExperimentService;
import com.university.booking.web.AdminOnly;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/cassandra")
@AllArgsConstructor
public class CassandraAdminController {

    private final ClusterStatusService clusterStatusService;
    private final ConsistencyExperimentService consistencyExperimentService;
    private final LoadExperimentService loadExperimentService;
    private final HotPartitionExperimentService hotPartitionExperimentService;
    private final EventStore eventStore;

    @AdminOnly
    @GetMapping("/status")
    public ResponseEntity<ClusterStatus> status() {
        return ResponseEntity.ok(clusterStatusService.status());
    }

    @AdminOnly
    @GetMapping("/buckets")
    public ResponseEntity<List<BucketInfo>> buckets(@RequestParam("stream") String stream) {
        return ResponseEntity.ok(eventStore.buckets(stream));
    }

    @AdminOnly
    @PostMapping("/experiments/consistency")
    public ResponseEntity<ConsistencyReport> consistency(
            @RequestParam(name = "levels", defaultValue = "ONE,ALL") List<String> levels,
            @RequestParam(name = "ops", defaultValue = "200") int ops,
            @RequestParam(name = "mode", defaultValue = "batch") String mode) {
        return ResponseEntity.ok(consistencyExperimentService.run(levels, ops, mode));
    }

    @AdminOnly
    @PostMapping("/experiments/load")
    public ResponseEntity<LoadReport> load(
            @RequestParam(name = "kind", defaultValue = "write") String kind,
            @RequestParam(name = "ops", defaultValue = "20000") int ops,
            @RequestParam(name = "concurrency", defaultValue = "64") int concurrency,
            @RequestParam(name = "consistency", required = false) String consistency,
            @RequestParam(name = "writeRatio", defaultValue = "0.5") double writeRatio) {
        return ResponseEntity.ok(loadExperimentService.run(kind, ops, concurrency, consistency, writeRatio));
    }

    @AdminOnly
    @PostMapping("/experiments/hot-partition/fill")
    public ResponseEntity<HotPartitionFillReport> hotPartitionFill(
            @RequestParam(name = "count", defaultValue = "300000") int count,
            @RequestParam(name = "concurrency", defaultValue = "128") int concurrency) {
        return ResponseEntity.ok(hotPartitionExperimentService.fill(count, concurrency));
    }

    @AdminOnly
    @GetMapping("/experiments/hot-partition/report")
    public ResponseEntity<HotPartitionReport> hotPartitionReport(
            @RequestParam(name = "repeats", defaultValue = "30") int repeats) {
        return ResponseEntity.ok(hotPartitionExperimentService.report(repeats));
    }

    @AdminOnly
    @PostMapping("/experiments/hot-partition/reset")
    public ResponseEntity<HotPartitionResetReport> hotPartitionReset() {
        return ResponseEntity.ok(hotPartitionExperimentService.reset());
    }
}
