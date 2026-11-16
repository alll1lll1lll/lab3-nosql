package com.university.booking.events;

import com.university.booking.enums.BookingStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EventType {
    BOOKING_CREATED(EntityType.BOOKING, "заявка создана"),
    BOOKING_SUBMITTED(EntityType.BOOKING, "заявка отправлена на рассмотрение"),
    BOOKING_APPROVED(EntityType.BOOKING, "заявка одобрена"),
    BOOKING_REJECTED(EntityType.BOOKING, "заявка отклонена"),
    BOOKING_DELETED(EntityType.BOOKING, "заявка удалена"),
    BOOKING_COMMENT_ADDED(EntityType.BOOKING, "добавлен комментарий"),
    BOOKING_COMMENT_DELETED(EntityType.BOOKING, "удалён комментарий"),
    ROOM_CREATED(EntityType.ROOM, "помещение создано"),
    ROOM_DELETED(EntityType.ROOM, "помещение удалено"),
    CATEGORY_CREATED(EntityType.CATEGORY, "категория создана"),
    CATEGORY_DELETED(EntityType.CATEGORY, "категория удалена"),
    PERSON_CREATED(EntityType.PERSON, "пользователь зарегистрирован"),
    PERSON_DELETED(EntityType.PERSON, "пользователь удалён"),
    ORGANIZATION_CREATED(EntityType.ORGANIZATION, "организация создана"),
    ORGANIZATION_UPDATED(EntityType.ORGANIZATION, "организация изменена"),
    ORGANIZATION_DELETED(EntityType.ORGANIZATION, "организация удалена"),

    CONSISTENCY_PROBE(EntityType.EXPERIMENT, "зонд эксперимента согласованности"),
    LOAD_TEST(EntityType.EXPERIMENT, "событие нагрузочного теста"),
    HOT_PARTITION_DEMO(EntityType.EXPERIMENT, "событие эксперимента hot partition");

    private final EntityType entityType;
    private final String title;

    public static EventType forBookingStatus(BookingStatus status) {
        return switch (status) {
            case UNDER_REVIEW -> BOOKING_SUBMITTED;
            case APPROVED -> BOOKING_APPROVED;
            case REJECTED -> BOOKING_REJECTED;
            default -> throw new IllegalArgumentException("no event for booking status " + status);
        };
    }
}
