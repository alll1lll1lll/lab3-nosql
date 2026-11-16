package com.university.booking.dto.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventPage(
        List<EventView> items,
        int size,
        String nextPageToken,
        List<Integer> bucketsScanned,
        String consistency,
        String coordinator,
        double tookMs) {
}
