package com.university.booking.dto.cassandra;

import java.util.List;
import java.util.Map;

public record HotPartitionFillReport(
        int events,
        List<Long> seqRange,
        List<Integer> buckets,
        double durationSec,
        double writesPerSec,
        Map<String, LatencyStats> writes) {
}
