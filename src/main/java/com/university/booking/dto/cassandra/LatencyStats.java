package com.university.booking.dto.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

public record LatencyStats(
        int total,
        int ok,
        int failed,
        double successRate,
        Double avgMs,
        Double p50Ms,
        Double p95Ms,
        Double p99Ms,
        Double maxMs,
        Double failureAvgMs,
        Double failureP95Ms,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Integer> errors,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> errorSamples) {
}
