package com.university.booking.events;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import java.time.Instant;

public record EventQuery(
        Instant from,
        Instant to,
        boolean ascending,
        int size,
        String pageToken,
        ConsistencyLevel consistency) {
}
