package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ItemReferences;
import com.jrobertgardzinski.collections.domain.ItemRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A heap-only {@link CollectionStore} for the tests that want no JDBC at all (application-layer
 * scenarios, the HTTP scenarios' store). The running service never uses it: {@link Main} always
 * wires {@link JdbcCollectionStore}, over in-memory H2 when no {@code DB_URL} is set. A {@link
 * LinkedHashSet} per (user, collection) gives set semantics (idempotent save) while remembering
 * insertion order for a newest-first listing. Coarse {@code synchronized} methods are the whole
 * concurrency story, so a plain {@link HashMap} underneath suffices.
 */
public class InMemoryCollectionStore implements CollectionStore, ItemReferences {

    private record Key(String user, String collection) {
    }

    private final Map<Key, LinkedHashSet<ItemRef>> data = new HashMap<>();

    @Override
    public synchronized boolean add(String user, String collection, ItemRef item) {
        return data.computeIfAbsent(new Key(user, collection), k -> new LinkedHashSet<>()).add(item);
    }

    @Override
    public synchronized boolean remove(String user, String collection, ItemRef item) {
        LinkedHashSet<ItemRef> set = data.get(new Key(user, collection));
        return set != null && set.remove(item);
    }

    @Override
    public synchronized List<ItemRef> list(String user, String collection) {
        LinkedHashSet<ItemRef> set = data.get(new Key(user, collection));
        if (set == null) {
            return List.of();
        }
        List<ItemRef> newestFirst = new ArrayList<>(set);
        Collections.reverse(newestFirst);
        return newestFirst;
    }

    @Override
    public synchronized int purgeUser(String user) {
        int removed = 0;
        var iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().user().equals(user)) {
                removed += entry.getValue().size();
                iterator.remove();
            }
        }
        return removed;
    }

    /**
     * The item axis, the heap's answer to V2's index: every (user, collection) bucket loses the
     * doomed refs. A full walk of the map is the honest in-memory equivalent of a table scan —
     * this store exists for tests, where the map holds a handful of entries; the index that makes
     * the same question cheap lives in the migration, not here.
     */
    @Override
    public synchronized int purge(String itemType, List<String> itemIds) {
        Set<ItemRef> doomed = new HashSet<>();
        for (String itemId : itemIds) {
            doomed.add(new ItemRef(itemType, itemId));
        }
        int removed = 0;
        for (LinkedHashSet<ItemRef> refs : data.values()) {
            for (ItemRef ref : doomed) {
                if (refs.remove(ref)) {
                    removed++;
                }
            }
        }
        return removed;
    }
}
