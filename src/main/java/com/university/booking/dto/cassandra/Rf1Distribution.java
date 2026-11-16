package com.university.booking.dto.cassandra;

import java.util.Map;

public record Rf1Distribution(Map<String, Long> bucketed, Map<String, Long> unbucketed) {
}
