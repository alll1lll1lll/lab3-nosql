package com.university.booking.events;

import com.datastax.oss.driver.api.core.cql.Row;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.enums.RedisKeyPrefix;
import com.university.booking.exception.ServiceUnavailableException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, Integer> lastKnownBucket = new ConcurrentHashMap<>();

    public int bucketOf(long seq) {
        return (int) ((seq - 1) / bucketSize());
    }

    public BucketAllocation allocate(String stream) {
        return reserve(stream, 1).stream()
                .mapToObj(seq -> toAllocation(stream, seq))
                .findFirst()
                .orElseGet(() -> fallback(stream));
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
            Long end = redis.opsForValue().increment(key, count);
            if (end == null) {
                return OptionalLong.empty();
            }
            if (end == count) {
                end = continueAfterCatalog(stream, key, end);
            }
            lastKnownBucket.put(stream, bucketOf(end));
            return OptionalLong.of(end - count + 1);
        } catch (RuntimeException e) {
            log.atWarn().addKeyValue("event", "bucket_counter_unavailable").addKeyValue("stream", stream)
                    .log("Redis unavailable, falling back to last known bucket: {}", e.getMessage());
            return OptionalLong.empty();
        }
    }

    private long continueAfterCatalog(String stream, String key, long end) {
        Optional<Integer> last = lastBucketInCatalog(stream);
        if (last.isEmpty()) {
            return end;
        }
        Long shifted = redis.opsForValue().increment(key, (last.get() + 1L) * bucketSize());
        log.atWarn().addKeyValue("event", "bucket_counter_recovered").addKeyValue("stream", stream)
                .log("sequence counter was lost in Redis, continuing from bucket {}", last.get() + 1);
        return shifted != null ? shifted : end;
    }

    private BucketAllocation fallback(String stream) {
        int bucket = lastKnownBucket.computeIfAbsent(stream, s -> lastBucketInCatalog(s).orElse(0));
        return BucketAllocation.withoutSeq(stream, bucket, true);
    }

    private Optional<Integer> lastBucketInCatalog(String stream) {
        try {
            CassandraConnector.Connection c = cassandra.require();
            Row row = c.session().execute(c.statements().selectLastBucket().bind(stream)).one();
            return row == null ? Optional.empty() : Optional.of(row.getInt("bucket"));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private long bucketSize() {
        return cassandra.properties().bucketSize();
    }
}
