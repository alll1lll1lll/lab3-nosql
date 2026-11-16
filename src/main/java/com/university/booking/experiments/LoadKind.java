package com.university.booking.experiments;

import com.university.booking.exception.ValidationException;

public enum LoadKind {
    WRITE,
    READ,
    MIXED;

    public static LoadKind parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ValidationException("kind должен быть write, read или mixed");
        }
    }

    public double writeRatio(double requested) {
        return switch (this) {
            case WRITE -> 1.0;
            case READ -> 0.0;
            case MIXED -> Math.clamp(requested, 0.0, 1.0);
        };
    }
}
