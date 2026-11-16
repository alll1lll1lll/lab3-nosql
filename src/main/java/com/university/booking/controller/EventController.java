package com.university.booking.controller;

import com.university.booking.dto.event.EventPage;
import com.university.booking.dto.event.EventQueryParams;
import com.university.booking.dto.event.EventTypeView;
import com.university.booking.service.event.EventQueryService;
import com.university.booking.web.AdminOnly;
import com.university.booking.web.PersonAuthInterceptor;
import java.util.List;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
@AllArgsConstructor
public class EventController {

    private final EventQueryService eventQueryService;

    @GetMapping("/types")
    public ResponseEntity<List<EventTypeView>> types() {
        return ResponseEntity.ok(eventQueryService.types());
    }

    @AdminOnly
    @GetMapping("/by-type")
    public ResponseEntity<EventPage> byType(@RequestParam("type") String type,
                                            @ModelAttribute EventQueryParams params) {
        return ResponseEntity.ok(eventQueryService.byType(type, params));
    }

    @GetMapping("/by-entity/{entityType}/{entityId}")
    public ResponseEntity<EventPage> byEntity(@RequestAttribute(PersonAuthInterceptor.PERSON_ID_ATTRIBUTE) String personId,
                                              @PathVariable("entityType") String entityType,
                                              @PathVariable("entityId") String entityId,
                                              @ModelAttribute EventQueryParams params) {
        return ResponseEntity.ok(eventQueryService.byEntity(personId, entityType, entityId, params));
    }

    @GetMapping("/by-actor/{actorId}")
    public ResponseEntity<EventPage> byActor(@RequestAttribute(PersonAuthInterceptor.PERSON_ID_ATTRIBUTE) String personId,
                                             @PathVariable("actorId") String actorId,
                                             @ModelAttribute EventQueryParams params) {
        return ResponseEntity.ok(eventQueryService.byActor(personId, actorId, params));
    }
}
