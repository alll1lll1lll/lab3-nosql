package com.university.booking.experiments;

import com.university.booking.dto.cassandra.LatencyStats;
import com.university.booking.util.Timings;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;

final class LatencySeries {

    private final long[] nanos;
    private final Map<String, Integer> errors = new HashMap<>();
    private final Map<String, String> errorSamples = new HashMap<>();
    private final java.util.List<Long> failedNanos = new java.util.ArrayList<>();
    private int okCount;
    private int total;

    LatencySeries(int capacity) {
        this.nanos = new long[capacity];
    }

    synchronized void ok(long elapsedNanos) {
        if (okCount < nanos.length) {
            nanos[okCount++] = elapsedNanos;
        }
        total++;
    }

    synchronized void fail(Throwable error, long elapsedNanos) {
        failedNanos.add(elapsedNanos);
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        String name = cause.getClass().getSimpleName();
        errors.merge(name, 1, Integer::sum);
        errorSamples.putIfAbsent(name, String.valueOf(cause.getMessage()));
        total++;
    }

    synchronized boolean isEmpty() {
        return total == 0;
    }

    synchronized LatencyStats stats() {
        long[] sorted = Arrays.copyOf(nanos, okCount);
        Arrays.sort(sorted);
        double avgNanos = Arrays.stream(sorted).average().orElse(0);
        return new LatencyStats(total, okCount, total - okCount,
                Timings.round(total == 0 ? 0 : 100.0 * okCount / total),
                okCount == 0 ? null : Timings.toMillis(Math.round(avgNanos)),
                percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99),
                okCount == 0 ? null : Timings.toMillis(sorted[okCount - 1]),
                failureAverage(), failureP95(),
                new TreeMap<>(errors), new TreeMap<>(errorSamples));
    }

    private Double failureAverage() {
        return failedNanos.isEmpty() ? null : Timings.toMillis(Math.round(
                failedNanos.stream().mapToLong(Long::longValue).average().orElseThrow()));
    }

    private Double failureP95() {
        return percentile(failedNanos.stream().mapToLong(Long::longValue).sorted().toArray(), 95);
    }

    private static Double percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return null;
        }
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return Timings.toMillis(sorted[Math.max(0, rank - 1)]);
    }
}
