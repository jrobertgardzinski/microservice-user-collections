package com.jrobertgardzinski.collections.application;

import java.util.List;

/**
 * The deletion-cascade axis for this service: a thing somewhere else in the portal is gone, so
 * every reference pointing at it must go too — whoever saved it, in whatever collection.
 *
 * <p>The contrast with {@link PurgeUserItems} is the whole point of this use case existing
 * separately. That one serves the account-deletion SAGA: an orchestrator is waiting for a
 * confirmation, a missed purge is a broken GDPR promise, so the command is retried forever. This
 * one serves a CHOREOGRAPHY: nobody is waiting, nobody confirms, nothing compensates. If it never
 * runs, the worst that survives is a row pointing at something that no longer exists — a dead
 * reference the UI is built to render honestly (see collections-ui) rather than an inconsistency
 * anyone can be harmed by.
 *
 * <p>Idempotent per ADR 0006, and trivially so: the second execution matches no rows and returns
 * 0 without an error. That is what lets the consumers commit offsets freely and lets a redelivered
 * event cost nothing.
 */
public class PurgeDeletedItem {

    private final ItemReferences references;

    public PurgeDeletedItem(ItemReferences references) {
        this.references = references;
    }

    /**
     * Removes every reference to any of {@code itemIds} of type {@code itemType}; returns how many
     * refs went (for the log/trace).
     *
     * <p>Blanks and duplicates are dropped HERE rather than in the adapter: one event may name the
     * same comment twice (an at-least-once producer merging two batches), and a duplicated id
     * would otherwise widen the SQL {@code IN} list for nothing. A blank id would match nothing
     * anyway, but letting it through would make the returned count harder to read than it needs
     * to be. Nothing left to purge is 0, never an exception — the caller is a best-effort cascade,
     * not a command that may fail.
     */
    public int execute(String itemType, List<String> itemIds) {
        if (itemType == null || itemType.isBlank() || itemIds == null) {
            return 0;
        }
        List<String> ids = itemIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .distinct()
                .toList();
        if (ids.isEmpty()) {
            return 0;
        }
        return references.purge(itemType, ids);
    }
}
