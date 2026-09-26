package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.identity.UserId;

import java.util.List;
import java.util.Optional;

/**
 * The persistence port. A user owns named collections (e.g. {@code "favourites"}, {@code "saved"});
 * each holds a set of {@link ItemRef}s. Membership is a set — saving the same ref twice is a no-op —
 * which is what makes the save idempotent.
 *
 * <p>This is the OWNER'S world, and its promise is absolute: nothing it lists is pending erasure,
 * because the adapter reads the {@code active_collection_items} view and never the table. The
 * account-deletion saga lives entirely behind {@link ItemErasure} — including the deletion, which
 * used to be a {@code purgeUser(user)} method here. It was removed rather than left unused: a
 * wholesale "delete everything this user has" is exactly the operation that made this participant
 * impossible to compensate, and leaving it within reach invites the next caller to bypass the saga.
 */
public interface CollectionRepository {

    /**
     * Adds the ref to the user's collection; returns true only if it was not already there. The id
     * is the owner's stable identity and is stored beside the address; empty for a token that
     * predates it.
     */
    boolean add(String user, Optional<UserId> userId, String collection, ItemRef item);

    /** The same save for a caller that knows only the address. */
    default boolean add(String user, String collection, ItemRef item) {
        return add(user, Optional.empty(), collection, item);
    }

    /** Removes the ref; returns true only if it was there. */
    boolean remove(String user, String collection, ItemRef item);

    /** The refs in the user's collection, newest first — ACTIVE ones, which are the only kind. */
    List<ItemRef> list(String user, String collection);
}
