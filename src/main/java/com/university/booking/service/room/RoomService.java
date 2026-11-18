package com.university.booking.service.room;

import com.university.booking.dto.room.RoomRequest;
import com.university.booking.dto.room.RoomScheduleResponse;
import com.university.booking.enums.BookingStatus;
import com.university.booking.events.EventDraft;
import com.university.booking.events.EventPayload;
import com.university.booking.events.EventRecorder;
import com.university.booking.events.EventType;
import com.university.booking.exception.ResourceNotFoundException;
import com.university.booking.model.Room;
import com.university.booking.repository.BookingRepository;
import com.university.booking.repository.RoomRepository;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class RoomService {

    private final RoomRepository roomRepository;
    private final BookingRepository bookingRepository;
    private final EventRecorder events;

    @CacheEvict(value = "rooms", allEntries = true)
    public Room createRoom(RoomRequest request) {
        Room room = Room.builder()
                .id(UUID.randomUUID().toString())
                .name(request.getName())
                .type(request.getType())
                .capacity(request.getCapacity())
                .location(request.getLocation())
                .teacherOnly(request.isTeacherOnly())
                .build();

        Room saved = roomRepository.save(room);
        events.record(EventDraft.of(EventType.ROOM_CREATED, saved.getId(), EventPayload.of(
                "name", saved.getName(), "type", saved.getType(), "capacity", saved.getCapacity(),
                "location", saved.getLocation(), "teacherOnly", saved.isTeacherOnly())));
        return saved;
    }

    public Room getRoom(String id) {
        return roomRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(id));
    }

    public RoomScheduleResponse getSchedule(String roomId, LocalDate date) {
        Room room = getRoom(roomId);
        List<RoomScheduleResponse.BusySlot> busy = bookingRepository
                .findByRoomIdAndEventDateAndStatusOrderByStartTimeAsc(room.getId(), date, BookingStatus.APPROVED)
                .stream()
                .map(b -> new RoomScheduleResponse.BusySlot(b.getStartTime(), b.getEndTime()))
                .toList();
        return new RoomScheduleResponse(room.getId(), date, busy);
    }

    @Cacheable(value = "rooms")
    public List<Room> getAllRooms() {
        return roomRepository.findAll();
    }

    @CacheEvict(value = "rooms", allEntries = true)
    public void deleteRoom(String id) {
        Optional<Room> deleted = roomRepository.findById(id);
        roomRepository.deleteById(id);
        deleted.ifPresent(room -> events.record(EventDraft.of(EventType.ROOM_DELETED, id,
                EventPayload.of("name", room.getName(), "location", room.getLocation()))));
    }
}
