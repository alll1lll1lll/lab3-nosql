package com.university.booking.dto.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

public record LatencyStats(
        int total,
        int ok,
        int failed,
        double successRate,
        double avgMs,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        double maxMs,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Integer> errors,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> errorSamples) {
}
