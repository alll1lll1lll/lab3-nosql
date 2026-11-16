package com.university.booking.util;

public final class Timings {

    private static final double NANOS_PER_MILLI = 1e6;
    private static final double NANOS_PER_SECOND = 1e9;

    private Timings() {
    }

    public static double millisSince(long startedNanos) {
        return toMillis(System.nanoTime() - startedNanos);
    }

    public static double secondsSince(long startedNanos) {
        return (System.nanoTime() - startedNanos) / NANOS_PER_SECOND;
    }

    public static double toMillis(long nanos) {
        return round(nanos / NANOS_PER_MILLI);
    }

    public static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
