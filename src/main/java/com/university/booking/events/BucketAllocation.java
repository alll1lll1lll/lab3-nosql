package com.university.booking.events;

public record BucketAllocation(String stream, long seq, int bucket, boolean first) {

    private static final long NO_SEQ = -1;

    public static BucketAllocation withoutSeq(String stream, int bucket, boolean first) {
        return new BucketAllocation(stream, NO_SEQ, bucket, first);
    }

    public boolean hasSeq() {
        return seq > 0;
    }
}
