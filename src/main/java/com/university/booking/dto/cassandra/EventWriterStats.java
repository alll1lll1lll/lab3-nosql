package com.university.booking.dto.cassandra;

public record EventWriterStats(long written, int pendingRetry, long failedAttempts, long dropped) {
}
