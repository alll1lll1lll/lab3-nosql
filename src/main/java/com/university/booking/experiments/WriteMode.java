package com.university.booking.experiments;

import com.university.booking.exception.ValidationException;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WriteMode {
    BATCH("LOGGED batch into 3 tables"),
    SINGLE("single insert into events_by_type");

    private final String description;

    public static WriteMode parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ValidationException("mode должен быть batch или single");
        }
    }
}
