package com.university.booking.dto.cassandra;

public record ConsistencyLevelResult(
        int requiredReplicas,
        LatencyStats write,
        LatencyStats read,
        int readYourWrites) {
}
