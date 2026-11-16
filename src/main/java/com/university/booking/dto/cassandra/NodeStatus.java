package com.university.booking.dto.cassandra;

public record NodeStatus(
        String node,
        String endpoint,
        String broadcast,
        String datacenter,
        String rack,
        String state,
        int openConnections,
        String version,
        String hostId) {
}
