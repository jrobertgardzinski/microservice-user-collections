package com.jrobertgardzinski.collections.config;

import java.time.Duration;
import java.time.Instant;

/**
 * How long a saved reference may stay marked for erasure before the mark stops meaning "a saga is
 * working on it" and starts meaning "the closure never came".
 *
 * <p>The first inhabitant of a config layer this service did not have: it kept its policy as
 * constants among the adapters, which was fine while every number was technical. This one is not.
 * It trades a false alarm during a slow deployment against the hours a genuinely lost erasure stays
 * unnoticed, and that trade belongs to whoever answers for the obligation — the same reason its
 * twins in memes and comments sit in config.
 *
 * <p>The default is derived, not felt, and identical to theirs: the orchestrator gives a purge 120s
 * and re-commands it three times, so a case is decided within about eight minutes. Thirty leaves
 * room for a slow deployment and still raises a lost closure inside the same shift.
 */
public record ErasureTolerance(Duration markStandsFor) {

    public static final ErasureTolerance DEFAULT = new ErasureTolerance(Duration.ofMinutes(30));

    public ErasureTolerance {
        if (markStandsFor.isNegative() || markStandsFor.isZero()) {
            throw new IllegalArgumentException("a mark must be allowed to stand for some time, was "
                    + markStandsFor);
        }
    }

    /** Marks older than this instant are evidence of a lost closure, not of a running saga. */
    public Instant overdueBefore(Instant now) {
        return now.minus(markStandsFor);
    }
}
