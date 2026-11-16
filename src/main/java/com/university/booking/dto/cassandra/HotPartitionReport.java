package com.university.booking.dto.cassandra;

import java.util.List;
import java.util.Map;

public record HotPartitionReport(
        long bucketSize,
        List<PartitionReport> bucketed,
        PartitionReport unbucketed,
        Rf1Distribution rf1RowsPerNode,
        Map<String, Integer> rf1OwnerOfBuckets0to29,
        ReadLatencyReport readLatencyAtOne) {
}
