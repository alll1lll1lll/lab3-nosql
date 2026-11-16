package com.university.booking.dto.cassandra;

import java.util.List;

public record PartitionReport(
        String partition,
        long rows,
        double countMs,
        List<String> replicasRf3,
        List<String> ownerRf1) {
}
