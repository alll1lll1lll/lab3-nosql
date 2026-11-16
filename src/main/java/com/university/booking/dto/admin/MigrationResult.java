package com.university.booking.dto.admin;

import java.time.LocalDateTime;

public record MigrationResult(String name, long matched, long modified, LocalDateTime executedAt) { }