package com.jrobertgardzinski.collections.application;

/**
 * The IRREVERSIBLE half of the account-deletion axis for this service: everything the leaver saved
 * is dropped, across every collection. It runs on the orchestrator's CLOSURE command — after every
 * participant has confirmed its reversible mark, so after the last moment at which the saga could
 * still decide to compensate.
 *
 * <p>It acts on exactly the references this service reserved ({@link ItemErasure#eraseMarked}),
 * never on "everything of that user": something saved after the mark belongs to no saga. Wholesale
 * within that set, though — unlike the meme and comment services this participant parses no purge
 * rule, because the refs are opaque and there is no per-item fate to decide.
 *
 * <p><strong>Where the pivot is, and where it is not.</strong> This service crosses no point of no
 * return of its own: its whole world is rows in one database, and a row deleted here is a row the
 * saga could in principle have kept. The saga's real pivot is in microservice-memes, at the moment
 * an image leaves object storage. That the orchestrator closes all three participants with the same
 * command is what makes the case irreversible at one instant rather than gradually.
 *
 * <p>Idempotent — the saga may deliver the closure twice, and the second delivery finds nothing
 * reserved.
 */
public class PurgeUserItems {

    private final ItemErasure erasure;

    public PurgeUserItems(ItemErasure erasure) {
        this.erasure = erasure;
    }

    /** Returns how many refs were removed (for the log/trace). */
    public int execute(String user) {
        return erasure.eraseMarked(user);
    }
}
