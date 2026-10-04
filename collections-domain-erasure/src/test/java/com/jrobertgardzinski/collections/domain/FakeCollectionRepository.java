package com.jrobertgardzinski.collections.domain;

import com.jrobertgardzinski.identity.UserId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory {@link CollectionRepository} for the tests that want no JDBC at all — this module's
 * own unit tests, {@code collections-infrastructure}'s HTTP scenarios and (via this module's
 * test-jar) portal-specs. The running service never uses it. A {@link LinkedHashSet} per
 * (user, collection) gives set semantics (idempotent save) while remembering insertion order for a
 * newest-first listing. Coarse {@code synchronized} methods are the whole concurrency story.
 *
 * <p>It implements {@link ItemErasure} as well, and the marks live in their OWN map rather than on
 * the refs — for the same reason the schema keeps the status on the row and the view does the
 * hiding: {@link #list} must not see a marked ref, and everything not in {@code marks} is ACTIVE.
 * {@link ItemErasureContractTest} holds this class and the JDBC adapter to the same promises.
 */
public class FakeCollectionRepository implements CollectionRepository, ItemReferences, ItemErasure {

    private record Key(UserId user, String collection) {
    }

    /** The natural key of a row — the same three fields the schema makes UNIQUE. */
    private record Row(UserId user, String collection, ItemRef ref) {
    }

    private final Map<Key, LinkedHashSet<ItemRef>> data = new HashMap<>();
    private final Map<Row, Instant> marks = new HashMap<>();

    @Override
    public synchronized boolean add(UserId user, String collection, ItemRef item) {
        return data.computeIfAbsent(new Key(user, collection), k -> new LinkedHashSet<>()).add(item);
    }

    @Override
    public synchronized boolean remove(UserId user, String collection, ItemRef item) {
        // a reserved row is not the owner's to remove, exactly as in the JDBC twin: it is invisible
        // in every listing, and destroying it would leave a compensation with nothing to restore
        if (marks.containsKey(new Row(user, collection, item))) {
            return false;
        }
        LinkedHashSet<ItemRef> set = data.get(new Key(user, collection));
        return set != null && set.remove(item);
    }

    @Override
    public synchronized List<ItemRef> list(UserId user, String collection) {
        LinkedHashSet<ItemRef> set = data.get(new Key(user, collection));
        if (set == null) {
            return List.of();
        }
        List<ItemRef> newestFirst = new ArrayList<>();
        for (ItemRef ref : set) {
            if (!marks.containsKey(new Row(user, collection, ref))) {
                newestFirst.add(ref);
            }
        }
        Collections.reverse(newestFirst);
        return newestFirst;
    }

    @Override
    public synchronized List<SavedItem> activeOf(UserId user) {
        return itemsOf(user, false);
    }

    @Override
    public synchronized List<SavedItem> pendingOf(UserId user) {
        return itemsOf(user, true);
    }

    private List<SavedItem> itemsOf(UserId user, boolean marked) {
        List<SavedItem> found = new ArrayList<>();
        for (Map.Entry<Key, LinkedHashSet<ItemRef>> entry : data.entrySet()) {
            if (!entry.getKey().user().equals(user)) {
                continue;
            }
            for (ItemRef ref : entry.getValue()) {
                Row row = new Row(user, entry.getKey().collection(), ref);
                if (marks.containsKey(row) == marked) {
                    found.add(itemOf(row));
                }
            }
        }
        return found;
    }

    @Override
    public synchronized void store(SavedItem state) {
        Row row = new Row(state.user(), state.collection(), state.ref());
        if (state.isPendingErasure()) {
            marks.put(row, state.markedForErasureAt());
        } else {
            marks.remove(row);
        }
    }

    @Override
    public synchronized int eraseMarked(UserId user) {
        int removed = 0;
        for (SavedItem reserved : pendingOf(user)) {
            LinkedHashSet<ItemRef> set = data.get(new Key(user, reserved.collection()));
            if (set != null && set.remove(reserved.ref())) {
                removed++;
            }
            marks.remove(new Row(user, reserved.collection(), reserved.ref()));
        }
        return removed;
    }

    @Override
    public synchronized List<SavedItem> pendingSince(Instant cutoff) {
        List<SavedItem> stuck = new ArrayList<>();
        for (Map.Entry<Row, Instant> mark : marks.entrySet()) {
            if (mark.getValue().isBefore(cutoff)) {
                stuck.add(itemOf(mark.getKey()));
            }
        }
        return stuck;
    }

    /** The reservations, for a test that fingerprints the whole world (the idempotence law). */
    public synchronized Map<String, Instant> marks() {
        Map<String, Instant> flat = new HashMap<>();
        marks.forEach((row, at) -> flat.put(
                row.user() + "/" + row.collection() + "/" + row.ref().itemType()
                        + ":" + row.ref().itemId(), at));
        return Map.copyOf(flat);
    }

    private SavedItem itemOf(Row row) {
        Instant marked = marks.get(row);
        return new SavedItem(row.user(), row.collection(), row.ref(),
                marked == null ? ItemStatus.ACTIVE : ItemStatus.PENDING_ERASURE, marked);
    }

    /** The item axis: every (user, collection) bucket loses the doomed refs, whatever their status. */
    @Override
    public synchronized int purge(String itemType, List<String> itemIds) {
        Set<ItemRef> doomed = new HashSet<>();
        for (String itemId : itemIds) {
            doomed.add(new ItemRef(itemType, itemId));
        }
        int removed = 0;
        for (Map.Entry<Key, LinkedHashSet<ItemRef>> entry : data.entrySet()) {
            for (ItemRef ref : doomed) {
                if (entry.getValue().remove(ref)) {
                    removed++;
                    marks.remove(new Row(entry.getKey().user(), entry.getKey().collection(), ref));
                }
            }
        }
        return removed;
    }
}
