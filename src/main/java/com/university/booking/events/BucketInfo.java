package com.university.booking.events;

import java.time.Instant;

public record BucketInfo(int bucket, Instant firstEventAt) {
}
