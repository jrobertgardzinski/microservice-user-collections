package com.jrobertgardzinski.collections.domain;

import java.time.Instant;

/**
 * One saved reference together with its erasure state: who saved it, into which collection, what it
 * points at, and whether a running account-deletion saga has it reserved.
 *
 * <p>{@link ItemRef} stays what it always was — an opaque pair the service never interprets — and
 * this record is the ROW around it. The natural key is (user, collection, ref), which the schema
 * has enforced as UNIQUE since V1, so nothing here needs the surrogate id: the same three fields
 * that identify a saved thing to its owner identify it to the erasure.
 *
 * <p><strong>Transitions are methods, never a setter</strong> (the rule this feature keeps in all
 * three participants). {@link #markForErasure(Instant)} and {@link #restore()} return a new value —
 * this is a record, the old state stays valid — and both are IDEMPOTENT, because Kafka delivers
 * at-least-once and each will be asked to do what it has already done. Marking twice keeps the
 * FIRST instant: the mark's age is what the backlog alarm is measured against, and a redelivered
 * command must not make an old obligation look fresh.
 */
public record SavedItem(String user, String collection, ItemRef ref, ItemStatus status,
                        Instant markedForErasureAt) {

    /**
     * The invariant, in the one place that can enforce it for values built in this process: a mark
     * and its timestamp exist together or not at all. The schema repeats it as a CHECK constraint,
     * because a row can also be written by a migration or by a human at a psql prompt, and neither
     * goes through this constructor.
     */
    public SavedItem {
        if ((status == ItemStatus.PENDING_ERASURE) != (markedForErasureAt != null)) {
            throw new IllegalArgumentException(
                    "a saved item is PENDING_ERASURE exactly when it carries the instant it was"
                            + " marked; got status=" + status
                            + ", markedForErasureAt=" + markedForErasureAt);
        }
    }

    /** A reference in somebody's list — the shorthand for every caller unrelated to erasure. */
    public SavedItem(String user, String collection, ItemRef ref) {
        this(user, collection, ref, ItemStatus.ACTIVE, null);
    }

    /**
     * The reversible half of an account deletion: the reference leaves every listing and stays a
     * row. Idempotent — a redelivered command finds it already marked and keeps the original
     * instant.
     */
    public SavedItem markForErasure(Instant at) {
        return status == ItemStatus.PENDING_ERASURE
                ? this
                : new SavedItem(user, collection, ref, ItemStatus.PENDING_ERASURE, at);
    }

    /**
     * The compensation: back into the list, exactly as before the mark. Idempotent for the same
     * reason as its opposite — and a no-op on an ACTIVE item rather than an error, because the
     * orchestrator compensating a saga cannot know which participants got as far as marking.
     */
    public SavedItem restore() {
        return status == ItemStatus.ACTIVE
                ? this
                : new SavedItem(user, collection, ref, ItemStatus.ACTIVE, null);
    }

    /** Whether a running saga has this reference reserved for erasure. */
    public boolean isPendingErasure() {
        return status == ItemStatus.PENDING_ERASURE;
    }
}
