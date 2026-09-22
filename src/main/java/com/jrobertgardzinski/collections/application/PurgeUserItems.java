package com.jrobertgardzinski.collections.application;

/**
 * The IRREVERSIBLE half of the account-deletion axis for this service: everything the leaver saved
 * AND THE SAGA RESERVED is dropped, across every collection. It runs on the orchestrator's CLOSURE
 * command — after every participant has confirmed its reversible mark, so after the last moment at
 * which the saga could still decide to compensate.
 *
 * <p>It acts on exactly the references this service reserved ({@link ItemErasure#eraseMarked}),
 * never on "everything of that user": something saved after the mark belongs to no saga. Wholesale
 * within that set, though — unlike the meme and comment services this participant parses no purge
 * rule, because the refs are opaque and there is no per-item fate to decide.
 *
 * <p><strong>What the closure leaves behind, and why it is said out loud.</strong> The mark hides a
 * leaver's rows and the closure destroys those rows — and nothing else. The design leaned on an
 * assumption made in ANOTHER service ("the account is locked for the whole deletion, so nobody can
 * save anything"), and it is not true here: this service's gate is offline, so an access token
 * already in a tab is accepted until its own {@code exp} — up to an hour after the deletion
 * started. A save in that window lands ACTIVE, the closure may not touch it, and nothing else ever
 * will: a row under the address of somebody who has been erased, invisible even to the backlog
 * alarm, which counts marks. So the closure counts what it had to leave and {@link
 * #execute(String)} returns it, for the participant to state. It is counted rather than destroyed
 * because "still here after the closure" and "saved by whoever holds this address NOW" are the same
 * row from in here, and re-registration is the case an over-eager closure would silently wipe.
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

    /**
     * What one closure did: how many reserved references it erased, and how many of that address's
     * references it had to leave behind (see the class javadoc — the residue of a write this
     * service could not refuse).
     */
    public record Closure(int erased, int leftBehind) {
    }

    private final ItemErasure erasure;

    public PurgeUserItems(ItemErasure erasure) {
        this.erasure = erasure;
    }

    public Closure execute(String user) {
        int erased = erasure.eraseMarked(user);
        // asked only of the delivery that actually closed a saga. A redelivery reserves nothing and
        // erases nothing, and counting then would report whatever that address holds today — which
        // after a re-registration is a different person's list — as a leaver's residue
        int leftBehind = erased == 0 ? 0 : erasure.activeOf(user).size();
        return new Closure(erased, leftBehind);
    }
}
