package com.university.booking.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.university.booking.cassandra.CassandraConnector;
import com.university.booking.config.CassandraProperties;
import com.university.booking.dto.event.EventView;
import com.university.booking.exception.ServiceUnavailableException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

class BucketBoundaryTest {

    @Test
    void boundaryAndUnavailableCounter() {
        CassandraConnector connector = mock(CassandraConnector.class);
        CassandraProperties properties = mock(CassandraProperties.class);
        when(connector.properties()).thenReturn(properties);
        when(properties.bucketSize()).thenReturn(100000L);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.hasKey(anyString())).thenThrow(new RuntimeException("offline"));
        BucketAllocator allocator = new BucketAllocator(redis, connector);
        assertEquals(0, allocator.bucketOf(100000));
        assertEquals(1, allocator.bucketOf(100001));
        assertFalse(allocator.toAllocation("type:test", 100000).first());
        assertTrue(allocator.toAllocation("type:test", 100001).first());
        assertThrows(ServiceUnavailableException.class, () -> allocator.allocate("type:test"));
    }

    private EventView event(int bucket, long millis) {
        return new EventView(Uuids.timeBased(), Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC),
                "TEST", "BOOKING", "1", "actor", "test", null, bucket, null);
    }

    @Test
    void overlappingBucketsAndEqualTimestampsPaginateInBothDirections() {
        List<EventView> input = List.of(event(0, 3000), event(1, 1000), event(1, 3000), event(0, 2000));
        for (boolean ascending : List.of(true, false)) {
            List<EventView> expected = StreamCursor.page(input, ascending, 100);
            List<EventView> seen = new ArrayList<>();
            StreamCursor cursor = null;
            while (true) {
                StreamCursor previous = cursor;
                List<EventView> remaining = input.stream()
                        .filter(e -> previous == null || previous.isAfter(e, ascending)).toList();
                List<EventView> page = StreamCursor.page(remaining, ascending, 1);
                if (page.isEmpty()) break;
                seen.addAll(page);
                cursor = StreamCursor.decode(StreamCursor.of(page.getFirst()).encode());
            }
            assertEquals(expected, seen);
            assertEquals(input.size(), seen.stream().map(EventView::eventId).distinct().count());
        }
    }
}
