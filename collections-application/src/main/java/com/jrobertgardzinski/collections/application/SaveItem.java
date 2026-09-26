package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.identity.UserId;


/**
 * Save a reference into one of a user's collections. Idempotent: saving what is already there
 * reports ALREADY_SAVED rather than failing, so a retried PUT is harmless.
 */
public class SaveItem {

    public enum Status { SAVED, ALREADY_SAVED }

    private final CollectionRepository store;

    public SaveItem(CollectionRepository store) {
        this.store = store;
    }

    public Status execute(UserId user, String collection, ItemRef item) {
        return store.add(user, collection, item) ? Status.SAVED : Status.ALREADY_SAVED;
    }
}
