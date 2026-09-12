package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.observation.Observations;
import com.jrobertgardzinski.collections.domain.Observation;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The one class in this service that knows what a metric is called.
 *
 * <p>Everything above it states facts ({@link Observation}); here they are given this month's
 * spelling — {@code collections_erasure_backlog}, {@code collections_kafka_records_dropped_total} —
 * and {@link MetricsEndpoint} reads them back out in Prometheus's text format. Swapping that format
 * for whatever comes next is rewriting these two files and nothing else.
 *
 * <p>Two translations neither of which is obvious from the fact alone: the backlog is a GAUGE (the
 * question is "how many right now", so it must fall back to zero by itself when a closure finally
 * lands), and a dropped command is a COUNTER (one increment is one saga command never carried out,
 * and an operator wants the running total, not a number that quietly forgets).
 *
 * <p>An INSTANCE, where both counters used to be static fields. The old note defending that —
 * "a Prometheus counter is per process anyway" — was true and still cost the usual price: two
 * classes reaching into each other's statics, and tests that had to remember to reset them. The
 * composition root holds one of these and hands it to everything that states a fact.
 *
 * <p>A backlog that could NOT be read states nothing at all, so the gauge keeps its last value —
 * reporting zero would turn a failed database read into "the backlog is clear".
 */
public final class ExportedObservations implements Observations<Observation> {

    private final AtomicLong erasureBacklog = new AtomicLong();
    private final AtomicLong recordsDropped = new AtomicLong();

    @Override
    public void record(Observation observation) {
        switch (observation) {
            case Observation.ErasureBacklog backlog -> erasureBacklog.set(backlog.marked());
            case Observation.SagaCommandDropped dropped -> recordsDropped.incrementAndGet();
        }
    }

    /** The gauge's current reading; zero until the first watch pass has run. */
    public long erasureBacklog() {
        return erasureBacklog.get();
    }

    /** The process-wide count of saga records abandoned after their retry budget. */
    public long recordsDropped() {
        return recordsDropped.get();
    }
}
