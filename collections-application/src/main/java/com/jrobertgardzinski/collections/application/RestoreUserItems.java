package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.SavedItem;

/**
 * The compensation: every reference this service reserved for a leaver goes back into their lists,
 * exactly as it was. This is what {@link MarkUserItemsForErasure} bought — before it, an
 * account-deletion saga that failed at another participant could apologise but not undo, because
 * this one had already deleted the rows.
 *
 * <p><strong>The orchestrator decides, not this service.</strong> There is no local timeout here
 * and no "the closure never came, let us assume the worst": a participant restoring on its own
 * clock would refill a leaver's list while the saga was still, correctly, waiting for a slow
 * sibling. The only trigger is the orchestrator's compensation command.
 *
 * <p><strong>Idempotent</strong> (workspace ADR 0006), and deliberately silent about how much it
 * found: a redelivered command restores nothing because the first one already did, and a command
 * for a saga this service never got as far as marking finds nothing at all. Neither is an error —
 * the orchestrator compensates every participant it commanded, and it cannot know which of them
 * heard the first command.
 */
public class RestoreUserItems {

    private final ItemErasure erasure;

    public RestoreUserItems(ItemErasure erasure) {
        this.erasure = erasure;
    }

    /** Returns how many references came back (zero on a redelivery, or if nothing was marked). */
    public int execute(String user) {
        int restored = 0;
        for (SavedItem item : erasure.pendingOf(user)) {
            erasure.store(item.restore());
            restored++;
        }
        return restored;
    }
}
