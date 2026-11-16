package com.university.booking.dto.cassandra;

import java.util.List;
import java.util.Map;

public record ClusterStatus(
        String clusterName,
        List<NodeStatus> nodes,
        Map<String, String> replication,
        String defaultConsistency,
        long bucketSize,
        EventWriterStats eventWriter) {
}
