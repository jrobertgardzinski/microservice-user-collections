package com.jrobertgardzinski.collections.application;

import java.util.List;

/**
 * The second persistence port, for the second access axis. {@link CollectionStore} is the
 * USER-owned view: every one of its methods takes the owner, because every HTTP route and the
 * account-deletion saga alike start from "whose data is this?". The deletion cascade starts from
 * the opposite end — a meme or a comment is gone, and every reference to it must go, no matter
 * whose collection it sits in — so it has no user to pass.
 *
 * <p><b>Why a separate port and not one more method on {@link CollectionStore}.</b> The two are
 * different questions with different indexes ({@code idx_collection_items_user} against
 * {@code idx_collection_items_item}, V1 and V2), different callers and different guarantees: the
 * saga's purge must never be lost, this one is best-effort. Keeping them apart means the saga's
 * port keeps the shape its contract test pins, and an adapter (or a test double) may serve one
 * axis without pretending to serve the other. The same table underneath is an implementation
 * detail of {@code JdbcCollectionStore}, not a reason to fuse the ports.
 */
public interface ItemReferences {

    /**
     * Removes every reference to any of these items, for every user and every collection; returns
     * how many rows went. Idempotent by nature — a second call simply matches nothing and returns
     * 0 — which is what lets the cascade absorb at-least-once delivery without dedup.
     *
     * <p>The caller has already dropped blanks and duplicates (see {@link PurgeDeletedItem}); an
     * empty list is a no-op.
     */
    int purge(String itemType, List<String> itemIds);
}
