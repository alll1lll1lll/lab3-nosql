package com.university.booking.dto.cassandra;

public record ReadComparison(LatencyStats bucketed, LatencyStats unbucketed) {
}
