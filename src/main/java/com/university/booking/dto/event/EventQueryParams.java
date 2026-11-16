package com.university.booking.dto.event;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EventQueryParams {

    private String from;
    private String to;
    private String order = "desc";
    private int size = 50;
    private String pageToken;
    private String consistency;
}
