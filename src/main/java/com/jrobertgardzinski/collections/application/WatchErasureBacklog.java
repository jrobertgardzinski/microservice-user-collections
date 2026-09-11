package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.config.ErasureTolerance;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.collections.domain.SavedItem;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Counts the obligations this service is sitting on — and does nothing about them on purpose.
 *
 * <p>A reference is marked {@code PENDING_ERASURE} by the saga's first, reversible step, and only
 * the orchestrator's closure turns that mark into a delete. A mark standing longer than any saga
 * can legitimately last therefore means the closure never arrived. The leaver's list looks empty,
 * which is what they asked for, but the rows are still there, which is not what the law asked for —
 * and nobody would find out: nothing is broken, nothing throws, a query simply returns fewer rows.
 *
 * <p>Erasing on a timer would mean guessing the orchestrator's decision, and the guess is wrong
 * exactly where it is expensive: a saga stuck because a sibling is down may still COMPENSATE, and
 * rows erased on a clock cannot come back. So the backlog is stated, and an operator acts on it.
 *
 * <p>It states the fact on EVERY pass, zero included — the question is "how many right now", so
 * saying nothing would leave yesterday's answer standing as if it were today's.
 */
public class WatchErasureBacklog {

    private final ItemErasure erasure;
    private final ErasureTolerance tolerance;
    private final Observations observations;
    private final Clock clock;

    public WatchErasureBacklog(ItemErasure erasure, ErasureTolerance tolerance,
                               Observations observations, Clock clock) {
        this.erasure = erasure;
        this.tolerance = tolerance;
        this.observations = observations;
        this.clock = clock;
    }

    /** Answers what it stated, so a caller can log the detail without asking the database twice. */
    public Observation.ErasureBacklog execute() {
        Instant now = clock.instant();
        List<SavedItem> overdue = erasure.pendingSince(tolerance.overdueBefore(now));
        if (overdue.isEmpty()) {
            observations.record(Observation.ErasureBacklog.NONE);
            return Observation.ErasureBacklog.NONE;
        }
        // the OLDEST mark decides how bad this is. Searched rather than taken from the head: unlike
        // its twins this store makes no promise about the order it hands the marks back in
        Instant oldestMark = overdue.stream().map(SavedItem::markedForErasureAt)
                .min(Instant::compareTo).orElseThrow();
        Observation.ErasureBacklog backlog = new Observation.ErasureBacklog(
                overdue.size(), Duration.between(oldestMark, now));
        observations.record(backlog);
        return backlog;
    }
}
