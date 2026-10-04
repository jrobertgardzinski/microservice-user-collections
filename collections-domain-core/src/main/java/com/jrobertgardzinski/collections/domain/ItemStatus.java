package com.jrobertgardzinski.collections.domain;

/**
 * Whether a saved reference is part of its owner's list or is waiting to be erased — the same two
 * values, for the same reason, as microservice-memes' {@code MemeStatus} and microservice-comments'
 * {@code CommentStatus}. An account deletion is a saga, and a participant that destroys on the
 * first command leaves the orchestrator with an apology instead of a compensation.
 *
 * <p>This service was the LAST of the three to learn it, and for a while that was written down as a
 * known debt (ADR 0007's consequences): memes and comments came back when a saga was given up on,
 * and the leaver's saved list did not. A private list is the least visible thing the portal holds
 * and the least defensible thing to lose — nobody else can tell it is gone.
 *
 * <p><strong>PENDING_ERASURE, not TO_DELETE</strong>: article 17 of the GDPR is the right to
 * erasure, and every artefact of this feature across the three services uses the regulation's word.
 */
public enum ItemStatus {

    /** In its owner's list: the only status any read of a collection may ever return. */
    ACTIVE,

    /**
     * Marked by a running account-deletion saga: gone from every listing, still a row, still
     * restorable by the saga's compensation. Nothing but the orchestrator's closure command turns
     * this into an actual deletion — never the mere passage of time.
     */
    PENDING_ERASURE
}
