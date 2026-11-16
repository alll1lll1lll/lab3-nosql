package com.university.booking.events;

import com.university.booking.exception.ValidationException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

record PageCursor(int bucket, ByteBuffer pagingState) {

    private static final char SEPARATOR = ':';

    String encode() {
        String state = pagingState == null ? "" : Base64.getEncoder().encodeToString(toBytes(pagingState));
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString((bucket + String.valueOf(SEPARATOR) + state).getBytes(StandardCharsets.UTF_8));
    }

    static PageCursor decode(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            int separator = raw.indexOf(SEPARATOR);
            int bucket = Integer.parseInt(raw.substring(0, separator));
            String state = raw.substring(separator + 1);
            return new PageCursor(bucket, state.isEmpty() ? null : ByteBuffer.wrap(Base64.getDecoder().decode(state)));
        } catch (RuntimeException e) {
            throw new ValidationException("Некорректный pageToken");
        }
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
