package com.university.booking.experiments;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

final class ConcurrentRunner {

    record Op(LatencySeries series, Supplier<? extends CompletionStage<?>> action) {
    }

    private ConcurrentRunner() {
    }

    static void run(Iterator<Op> ops, int concurrency) {
        Semaphore permits = new Semaphore(concurrency);
        try {
            while (ops.hasNext()) {
                Op op = ops.next();
                permits.acquireUninterruptibly();
                start(op, permits);
            }
        } finally {
            permits.acquireUninterruptibly(concurrency);
        }
    }

    private static void start(Op op, Semaphore permits) {
        long started = System.nanoTime();
        try {
            op.action().get().whenComplete((result, error) -> {
                if (error == null) {
                    op.series().ok(System.nanoTime() - started);
                } else {
                    op.series().fail(error, System.nanoTime() - started);
                }
                permits.release();
            });
        } catch (RuntimeException e) {
            op.series().fail(e, System.nanoTime() - started);
            permits.release();
        }
    }
}
