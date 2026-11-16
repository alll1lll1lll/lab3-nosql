package com.university.booking.dto.cassandra;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record LoadReport(
        String kind,
        int ops,
        int concurrency,
        String consistency,
        double durationSec,
        double throughputOpsPerSec,
        LatencyStats write,
        LatencyStats read) {
}
