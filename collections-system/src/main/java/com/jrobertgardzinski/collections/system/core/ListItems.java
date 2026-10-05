package com.jrobertgardzinski.collections.system.core;

import com.jrobertgardzinski.collections.domain.core.CollectionRepository;
import com.jrobertgardzinski.identity.UserId;
import com.jrobertgardzinski.collections.domain.core.ItemRef;

import java.util.List;

/**
 * List the refs in one of a user's collections. The service returns only the opaque refs; the UI
 * hydrates the details from each source service (a since-deleted item simply shows as unavailable).
 */
public class ListItems {

    private final CollectionRepository store;

    public ListItems(CollectionRepository store) {
        this.store = store;
    }

    public List<ItemRef> execute(UserId user, String collection) {
        return store.list(user, collection);
    }
}
