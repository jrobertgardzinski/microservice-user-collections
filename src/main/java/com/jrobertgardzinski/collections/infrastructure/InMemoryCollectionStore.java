package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.domain.ItemRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A heap-only {@link CollectionStore}: the default when no {@code DB_URL} is set (dev), and the
 * store the application-layer tests drive. A {@link LinkedHashSet} per (user, collection) gives set
 * semantics (idempotent save) while remembering insertion order for a newest-first listing.
 */
public class InMemoryCollectionStore implements CollectionStore {

    private record Key(String user, String collection) {
    }

    private final Map<Key, LinkedHashSet<ItemRef>> data = new ConcurrentHashMap<>();

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
}
