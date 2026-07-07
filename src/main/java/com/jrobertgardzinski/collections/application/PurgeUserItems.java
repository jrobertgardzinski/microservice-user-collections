package com.jrobertgardzinski.collections.application;

/**
 * The account-deletion axis for this service: drop everything a leaving user saved, across every
 * collection. Wholesale — there is no per-item policy to honour, because the refs are opaque, so
 * unlike the meme and comment services this participant does not parse a purge rule. Idempotent,
 * so at-least-once delivery of the saga command needs no dedup.
 */
public class PurgeUserItems {

    private final CollectionStore store;

    public PurgeUserItems(CollectionStore store) {
        this.store = store;
    }

    /** Returns how many refs were removed (for the log/trace). */
    public int execute(String user) {
        return store.purgeUser(user);
    }
}
