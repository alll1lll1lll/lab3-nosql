package com.university.booking.dto.cassandra;

public record HotPartitionResetReport(int deletedBuckets, String droppedKeyspace) {
}
