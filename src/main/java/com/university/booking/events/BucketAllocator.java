package com.university.booking.events;

import com.datastax.oss.driver.api.core.cql.Row;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.enums.RedisKeyPrefix;
import com.university.booking.exception.ServiceUnavailableException;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BucketAllocator {

    private final StringRedisTemplate redis;
    private final CassandraConnector cassandra;

    public int bucketOf(long seq) {
        return (int) ((seq - 1) / bucketSize());
    }

    public BucketAllocation allocate(String stream) {
        return toAllocation(stream, reserveOrFail(stream, 1));
    }

    public long reserveOrFail(String stream, long count) {
        return reserve(stream, count).orElseThrow(
                () -> new ServiceUnavailableException("Redis недоступен: нельзя выделить номера событий"));
    }

    public BucketAllocation toAllocation(String stream, long seq) {
        return new BucketAllocation(stream, seq, bucketOf(seq), (seq - 1) % bucketSize() == 0);
    }

    private OptionalLong reserve(String stream, long count) {
        String key = RedisKeyPrefix.EVENT_SEQUENCE.getKey(stream);
        try {
            long seed = -1;
            if (!Boolean.TRUE.equals(redis.hasKey(key))) {
                seed = lastBucketInCatalog(stream).map(b -> (b + 1L) * bucketSize()).orElse(0L);
            }
            Long end = redis.execute(new org.springframework.data.redis.core.script.DefaultRedisScript<>(
                    "if redis.call('EXISTS', KEYS[1]) == 0 then "
                    + "if tonumber(ARGV[1]) < 0 then return nil end "
                    + "redis.call('SET', KEYS[1], ARGV[1]) end "
                    + "return redis.call('INCRBY', KEYS[1], ARGV[2])", Long.class),
                    java.util.List.of(key), Long.toString(seed), Long.toString(count));
            if (end == null) return OptionalLong.empty();
            return OptionalLong.of(end - count + 1);
        } catch (RuntimeException e) {
            log.atWarn().addKeyValue("event", "bucket_counter_unavailable").addKeyValue("stream", stream)
                    .log("Redis unavailable, deferring event allocation: {}", e.getMessage());
            return OptionalLong.empty();
        }
    }

    private Optional<Integer> lastBucketInCatalog(String stream) {
        CassandraConnector.Connection c = cassandra.require();
        Row row = c.session().execute(c.statements().selectLastBucket().bind(stream)).one();
        return row == null ? Optional.empty() : Optional.of(row.getInt("bucket"));
    }

    private long bucketSize() {
        return cassandra.properties().bucketSize();
    }
}
