package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;

import java.util.List;

/**
 * The persistence port. A user owns named collections (e.g. {@code "favourites"}, {@code "saved"});
 * each holds a set of {@link ItemRef}s. Membership is a set — saving the same ref twice is a no-op —
 * which is what makes both the save and the account-deletion purge idempotent.
 */
public interface CollectionStore {

    /** Adds the ref to the user's collection; returns true only if it was not already there. */
    boolean add(String user, String collection, ItemRef item);

    /** Removes the ref; returns true only if it was there. */
    boolean remove(String user, String collection, ItemRef item);

    /** The refs in the user's collection, newest first. */
    List<ItemRef> list(String user, String collection);

    /** Removes every item the user has, across all collections; returns how many were removed. */
    int purgeUser(String user);
}
