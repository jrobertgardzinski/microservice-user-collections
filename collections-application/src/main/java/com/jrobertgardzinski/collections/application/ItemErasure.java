package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.SavedItem;

import java.time.Instant;
import java.util.List;

/**
 * The erasure-aware side of the store: the only port in this service that can see a saved reference
 * which is not {@link com.jrobertgardzinski.collections.domain.ItemStatus#ACTIVE}.
 *
 * <p><strong>Why a second port and not four more methods on {@link CollectionStore}.</strong>
 * {@link CollectionStore} is the owner's world, and its promise is absolute: nothing it lists is
 * pending erasure, because its adapter reads from the {@code active_collection_items} view and
 * never from the table. A port that could answer both questions would make that promise a matter of
 * which method you happened to call — and would leave the build-time guard
 * ({@code ItemReadFilterTest}) nothing to check.
 *
 * <p>Everything here is used by exactly three callers, all of them steps of the account-deletion
 * saga: the mark, its compensation, and the erasure the orchestrator's closure command triggers.
 */
public interface ItemErasure {

    /** The leaver's references still in their lists — what a mark has left to do. */
    List<SavedItem> activeOf(String user);

    /**
     * The leaver's references a running saga has already reserved — what a compensation restores
     * and what the erasure destroys. Both act on THIS set rather than on "everything of that
     * user", so neither can touch something saved after the mark.
     */
    List<SavedItem> pendingOf(String user);

    /**
     * Persist the erasure state the aggregate computed — {@code status} and
     * {@code markedForErasureAt}, nothing else on the row, addressed by the natural key. This is
     * the write half of "transitions are methods, not setters": the decision was made by
     * {@link SavedItem#markForErasure(Instant)} / {@link SavedItem#restore()} and all that is left
     * here is to store it. A row that no longer exists is not an error — the deletion cascade is a
     * legitimate way for a marked reference to disappear (the thing it pointed at was deleted, so
     * there is nothing to restore it to).
     */
    void store(SavedItem state);

    /**
     * Destroy exactly the references this service reserved for the leaver; returns how many went.
     *
     * <p>One statement rather than a loop over {@link #pendingOf(String)}, and that is the one
     * place this participant is deliberately unlike its two siblings: they decide each row's fate
     * separately, because a purge RULE reads each row's score. Here the refs are opaque — there is
     * no per-item policy to apply and therefore no per-item decision to make — so the set that was
     * reserved is exactly the set that goes.
     */
    int eraseMarked(String user);

    /**
     * Every reference marked before {@code cutoff} and still not erased — the reaper's query, and
     * the whole "structure" this feature has: a status and an instant, both on the row. Used to
     * WATCH the backlog (a mark older than any saga can legitimately last means a closure command
     * was lost), never to erase anything: nothing here deletes content because time passed.
     */
    List<SavedItem> pendingSince(Instant cutoff);
}
