package com.university.booking.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("app.cassandra")
public record CassandraProperties(
        @DefaultValue({"127.0.0.1:9042", "127.0.0.1:9043", "127.0.0.1:9044"}) List<String> contactPoints,
        @DefaultValue("dc1") String localDatacenter,
        @DefaultValue("booking_events") String keyspace,
        @DefaultValue("3") int replicationFactor,
        @DefaultValue("LOCAL_QUORUM") String consistency,
        @DefaultValue("5s") Duration requestTimeout,
        @DefaultValue("100000") long bucketSize,
        @DefaultValue("10s") Duration reconnectInterval) {
}
