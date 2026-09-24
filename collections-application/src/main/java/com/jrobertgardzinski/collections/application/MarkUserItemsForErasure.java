package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.SavedItem;

import java.time.Clock;
import java.time.Instant;

/**
 * This service's REVERSIBLE step of an account deletion: every reference the leaver still has in a
 * collection is marked {@link com.jrobertgardzinski.collections.domain.ItemStatus#PENDING_ERASURE}.
 * Nothing is destroyed — and that is the entire point: this is the step the orchestrator can take
 * back when a LATER participant of the same saga fails ({@link RestoreUserItems}).
 *
 * <p>What the leaver would see immediately is nevertheless the full effect of a deletion: their
 * lists are empty, because every listing reads through {@link CollectionStore}, whose adapter
 * cannot see a marked row.
 *
 * <p><strong>Idempotent</strong>, as every saga command must be (workspace ADR 0006): the command
 * arrives at least once, and the second delivery finds nothing left to mark — a reference that is
 * already marked keeps its ORIGINAL instant, so re-commanding never rejuvenates an obligation the
 * erasure backlog is watching.
 */
public class MarkUserItemsForErasure {

    private final ItemErasure erasure;
    private final Clock clock;

    public MarkUserItemsForErasure(ItemErasure erasure, Clock clock) {
        this.erasure = erasure;
        this.clock = clock;
    }

    /** Returns how many references were reserved by THIS delivery (zero on a redelivery). */
    public int execute(String user) {
        Instant at = Instant.now(clock);
        int marked = 0;
        for (SavedItem item : erasure.activeOf(user)) {
            // the transition is the aggregate's, never a setter and never an UPDATE spelled out
            // here: the record decides what "marked" means (including keeping the first instant on
            // a redelivery), and the port only stores the answer
            erasure.store(item.markForErasure(at));
            marked++;
        }
        return marked;
    }
}
