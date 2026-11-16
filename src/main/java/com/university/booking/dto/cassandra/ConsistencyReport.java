package com.university.booking.dto.cassandra;

import java.util.List;
import java.util.Map;

public record ConsistencyReport(
        int opsPerLevel,
        String writeMode,
        List<String> upNodes,
        Map<String, ConsistencyLevelResult> levels) {
}
