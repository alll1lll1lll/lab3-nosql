package com.university.booking.dto.cassandra;

public record ReadLatencyReport(
        ReadComparison newest100,
        ReadComparison oldest100,
        ReadComparison window100FromMiddle) {
}
