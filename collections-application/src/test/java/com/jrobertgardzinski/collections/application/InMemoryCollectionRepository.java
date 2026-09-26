package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.ItemStatus;
import com.jrobertgardzinski.collections.domain.SavedItem;
import com.jrobertgardzinski.identity.UserId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A heap-only {@link CollectionRepository} for the tests that want no JDBC at all — this module's own
 * unit tests, {@code collections-infrastructure}'s HTTP scenarios, and (via this module's test-jar)
 * account-closure-specs, which used to keep a hand-copied twin of this exact class for the same
 * reason and drifted from it (see {@link ItemErasureContractTest}, which both this class and the
 * real JDBC adapter answer to). The running service never uses it: {@code Main} always wires the
 * JDBC adapter, over in-memory H2 when no {@code DB_URL} is set. A {@link LinkedHashSet} per (user,
 * collection) gives set semantics (idempotent save) while remembering insertion order for a
 * newest-first listing. Coarse {@code synchronized} methods are the whole concurrency story, so a
 * plain {@link HashMap} underneath suffices.
 *
 * <p>It implements {@link ItemErasure} as well, and the marks live in their OWN map rather than on
 * the refs — for the same reason the schema keeps the status on the row and the view does the
 * hiding: {@link #list} must not see a marked ref, and everything not in {@code marks} is ACTIVE.
 * That mirrors {@code active_collection_items} exactly, which is what makes a scenario that runs
 * against this store mean something about the one that runs against Postgres.
 *
 * <p>Living here, next to {@link CollectionRepository}/{@link ItemErasure} and their contract test,
 * rather than in {@code collections-infrastructure}, is deliberate: this class is pure JDK, exactly
 * as framework-free as the ports it stands in for, and a test double belongs on the test classpath
 * of everyone who needs it, not inside the jar the running service ships.
 */
public class InMemoryCollectionRepository implements CollectionRepository, ItemReferences, ItemErasure {

    private record Key(String user, String collection) {
    }

    /** The natural key of a row — the same three fields V1 made UNIQUE. */
    private record Row(String user, String collection, ItemRef ref) {
    }

    private final Map<Key, LinkedHashSet<ItemRef>> data = new HashMap<>();
    private final Map<Row, Instant> marks = new HashMap<>();
    /** The owner's id per row — the {@code user_id} column; absent for a save without one. */
    private final Map<Row, UserId> ids = new HashMap<>();

    @Override
    public synchronized boolean add(String user, Optional<UserId> userId, String collection, ItemRef item) {
        boolean added = data.computeIfAbsent(new Key(user, collection), k -> new LinkedHashSet<>()).add(item);
        if (added) {
            userId.ifPresent(id -> ids.put(new Row(user, collection, item), id));
        }
        return added;
    }

    @Override
    public synchronized boolean remove(String user, String collection, ItemRef item) {
        // a reserved row is not the owner's to remove, exactly as in the JDBC twin: it is invisible
        // in every listing, and destroying it would leave a compensation with nothing to restore
        if (marks.containsKey(new Row(user, collection, item))) {
            return false;
        }
        LinkedHashSet<ItemRef> set = data.get(new Key(user, collection));
        boolean removed = set != null && set.remove(item);
        if (removed) {
            ids.remove(new Row(user, collection, item));
        }
        return removed;
    }

    @Override
    public synchronized List<ItemRef> list(String user, String collection) {
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
    public synchronized List<SavedItem> activeOf(String user) {
        return itemsOf(user, false);
    }

    @Override
    public synchronized List<SavedItem> pendingOf(String user) {
        return itemsOf(user, true);
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
        for (Map.Entry<Row, UserId> owned : ids.entrySet()) {
            if (owned.getValue().equals(user) && marks.containsKey(owned.getKey()) == marked) {
                found.add(itemOf(owned.getKey()));
            }
        }
        return found;
    }

    @Override
    public synchronized int eraseMarked(UserId user) {
        int removed = 0;
        for (SavedItem reserved : pendingOf(user)) {
            Row row = new Row(reserved.user(), reserved.collection(), reserved.ref());
            LinkedHashSet<ItemRef> set = data.get(new Key(row.user(), row.collection()));
            if (set != null && set.remove(row.ref())) {
                removed++;
            }
            marks.remove(row);
            ids.remove(row);
        }
        return removed;
    }

    private List<SavedItem> itemsOf(String user, boolean marked) {
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
    public synchronized int eraseMarked(String user) {
        int removed = 0;
        for (SavedItem reserved : pendingOf(user)) {
            LinkedHashSet<ItemRef> set = data.get(new Key(user, reserved.collection()));
            if (set != null && set.remove(reserved.ref())) {
                removed++;
            }
            marks.remove(new Row(user, reserved.collection(), reserved.ref()));
            ids.remove(new Row(user, reserved.collection(), reserved.ref()));
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

    /**
     * The reservations, for a test that fingerprints the whole world (the idempotence law). Without
     * it the law would only see what {@link #list} shows — and every erasure command would pass it
     * trivially, because a mark is invisible there BY DESIGN.
     */
    public synchronized Map<String, Instant> marks() {
        Map<String, Instant> flat = new HashMap<>();
        marks.forEach((row, at) -> flat.put(
                row.user() + "/" + row.collection() + "/" + row.ref().itemType()
                        + ":" + row.ref().itemId(), at));
        return Map.copyOf(flat);
    }

    private SavedItem itemOf(Row row) {
        Instant marked = marks.get(row);
        return new SavedItem(row.user(), Optional.ofNullable(ids.get(row)), row.collection(), row.ref(),
                marked == null ? ItemStatus.ACTIVE : ItemStatus.PENDING_ERASURE, marked);
    }

    /**
     * The item axis, the heap's answer to V2's index: every (user, collection) bucket loses the
     * doomed refs. A full walk of the map is the honest in-memory equivalent of a table scan —
     * this store exists for tests, where the map holds a handful of entries; the index that makes
     * the same question cheap lives in the migration, not here.
     *
     * <p>Status-blind on purpose, exactly like its JDBC twin: the cascade fires because the meme or
     * the comment is GONE, and a reference to something that no longer exists has nothing to be
     * restored to.
     */
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
                    Row row = new Row(entry.getKey().user(), entry.getKey().collection(), ref);
                    marks.remove(row);
                    ids.remove(row);
                }
            }
        }
        return removed;
    }
}
