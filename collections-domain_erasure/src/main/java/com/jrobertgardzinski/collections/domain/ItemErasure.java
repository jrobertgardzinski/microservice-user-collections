package com.jrobertgardzinski.collections.domain;

import com.jrobertgardzinski.identity.UserId;

import java.time.Instant;
import java.util.List;

/**
 * The saga's own view of the table, keyed by the owner's identity: the two halves of one member's
 * references, the one write the saga makes, the bulk erasure of what it marked, and the backlog
 * the reaper watches. Everything else reads the {@code active_collection_items} view.
 */
public interface ItemErasure {

    List<SavedItem> activeOf(UserId user);

    List<SavedItem> pendingOf(UserId user);

    /** Writes the erasure state of one reference — status and instant — and nothing else. */
    void store(SavedItem state);

    /** Destroys the marked references of one member; answers how many went. */
    int eraseMarked(UserId user);

    /** Marked strictly before the cutoff: the reaper's backlog. */
    List<SavedItem> pendingSince(Instant cutoff);
}
