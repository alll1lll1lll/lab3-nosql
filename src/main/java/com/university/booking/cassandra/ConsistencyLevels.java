package com.university.booking.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.university.booking.exception.ValidationException;

public final class ConsistencyLevels {

    private ConsistencyLevels() {
    }

    public static ConsistencyLevel parse(String name) {
        try {
            return DefaultConsistencyLevel.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Неизвестный уровень согласованности: " + name);
        }
    }

    public static ConsistencyLevel parseOrDefault(String name) {
        return name == null || name.isBlank() ? null : parse(name);
    }

    public static int requiredReplicas(ConsistencyLevel level, int replicationFactor) {
        return switch (level.name()) {
            case "TWO" -> 2;
            case "THREE" -> 3;
            case "QUORUM", "LOCAL_QUORUM", "EACH_QUORUM" -> replicationFactor / 2 + 1;
            case "ALL" -> replicationFactor;
            default -> 1;
        };
    }
}
