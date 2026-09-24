package com.jrobertgardzinski.collections.closure;

import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.SavedItem;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A heap-only store, written by hand rather than mocked, because everything this participant
 * answers is a COUNT and a count is only worth asserting when real rows moved between real states.
 * Rows are kept in insertion order; a reference is identified by (user, collection, ref), which is
 * what makes a second save of the same thing a no-op.
 *
 * <p>It lives here and not in collections-infrastructure's in-memory adapter on purpose: this
 * module must be provable with nothing below it, and borrowing an adapter would be borrowing the
 * very layer the module exists to do without.
 */
final class HeapStore implements CollectionStore, ItemErasure {

    private final List<SavedItem> rows = new ArrayList<>();

    private boolean same(SavedItem row, String user, String collection, ItemRef ref) {
        return row.user().equals(user) && row.collection().equals(collection) && row.ref().equals(ref);
    }

    @Override
    public boolean add(String user, String collection, ItemRef item) {
        if (rows.stream().anyMatch(row -> same(row, user, collection, item))) {
            return false;
        }
        return rows.add(new SavedItem(user, collection, item));
    }

    @Override
    public boolean remove(String user, String collection, ItemRef item) {
        return rows.removeIf(row -> same(row, user, collection, item));
    }

    @Override
    public List<ItemRef> list(String user, String collection) {
        return rows.stream()
                .filter(row -> row.user().equals(user) && row.collection().equals(collection))
                .filter(row -> !row.isPendingErasure())   // a marked reference is out of every list
                .map(SavedItem::ref)
                .toList();
    }

    @Override
    public List<SavedItem> activeOf(String user) {
        return rows.stream().filter(row -> row.user().equals(user))
                .filter(row -> !row.isPendingErasure()).toList();
    }

    @Override
    public List<SavedItem> pendingOf(String user) {
        return rows.stream().filter(row -> row.user().equals(user))
                .filter(SavedItem::isPendingErasure).toList();
    }

    @Override
    public void store(SavedItem state) {
        for (int i = 0; i < rows.size(); i++) {
            if (same(rows.get(i), state.user(), state.collection(), state.ref())) {
                rows.set(i, state);
                return;
            }
        }
        // and NOTHING when no row matches. The adapter runs `UPDATE … WHERE`, which quietly
        // matches nothing; appending here would hand a member a reference they never saved. The
        // port contract catches this, and caught exactly this.
    }

    @Override
    public int eraseMarked(String user) {
        List<SavedItem> doomed = pendingOf(user);
        rows.removeAll(doomed);
        return doomed.size();
    }

    @Override
    public List<SavedItem> pendingSince(Instant cutoff) {
        return rows.stream().filter(SavedItem::isPendingErasure)
                .filter(row -> row.markedForErasureAt().isBefore(cutoff)).toList();
    }

    /** Everything still held under this address, marked or not — what the closure has to clear. */
    List<SavedItem> all(String user) {
        return rows.stream().filter(row -> row.user().equals(user)).toList();
    }

    /** What this member would still see in their lists. */
    List<SavedItem> visible(String user) {
        return activeOf(user);
    }
}
