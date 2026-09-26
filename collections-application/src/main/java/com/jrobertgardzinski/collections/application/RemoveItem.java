package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.identity.UserId;
import com.jrobertgardzinski.collections.domain.ItemRef;

/**
 * Remove a reference from one of a user's collections. Idempotent: removing what is not there
 * reports NOT_SAVED rather than failing.
 */
public class RemoveItem {

    public enum Status { REMOVED, NOT_SAVED }

    private final CollectionRepository store;

    public RemoveItem(CollectionRepository store) {
        this.store = store;
    }

    public Status execute(UserId user, String collection, ItemRef item) {
        return store.remove(user, collection, item) ? Status.REMOVED : Status.NOT_SAVED;
    }
}
