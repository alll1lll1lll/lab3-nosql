package com.university.booking.experiments;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LatencySeriesTest {
    @Test void errorsHaveSeparateLatencyAndNoSuccessfulMeasurements() {
        LatencySeries series = new LatencySeries(2);
        series.fail(new IllegalStateException("unavailable"), 10_000_000);
        series.fail(new IllegalStateException("timeout"), 30_000_000);
        var stats = series.stats();
        assertNull(stats.avgMs());
        assertNull(stats.p95Ms());
        assertEquals(20.0, stats.failureAvgMs());
        assertEquals(30.0, stats.failureP95Ms());
        assertEquals(2, stats.failed());
        series.ok(5_000_000);
        assertEquals(5.0, series.stats().avgMs());
        assertEquals(20.0, series.stats().failureAvgMs());
    }
}
