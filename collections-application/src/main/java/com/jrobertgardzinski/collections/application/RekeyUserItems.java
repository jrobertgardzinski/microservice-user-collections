package com.jrobertgardzinski.collections.application;

/**
 * A member changed their e-mail address, and their saved references follow them.
 *
 * <p>Every row here is keyed by the address the token carried — the JWT's {@code sub} — and nothing
 * used to rewrite it, so a confirmed address change silently emptied a member's lists: {@code GET
 * /collections/favourites/items} answered {@code []} with no error, because the rows were still
 * there under a name nobody was asking by. The second half was worse and quieter: an account
 * deletion then reserved nothing, this service confirmed the erasure anyway, and the references
 * stayed for ever under an address the member no longer held.
 *
 * <p><strong>Idempotent</strong> by arithmetic rather than by bookkeeping, which is why there is no
 * dedup table: the second delivery of the same rename finds no row under the old address and moves
 * nothing. The fact is at-least-once like every other fact in this estate, and one UPDATE that
 * matches nothing is what it costs to absorb that.
 *
 * <p><strong>What it deliberately does NOT do: wait, or check.</strong> There is no verification
 * that the new address is free of rows, because there cannot be any — microservice-security refuses
 * a move onto a registered address, and a deletion takes its owner's rows with it. And there is no
 * ordering promise against the deletion saga: the rename travels on {@code security-events} while
 * the purge commands travel on {@code content-commands}, so a deletion requested seconds after a
 * rename can still overtake this. That window is the producer's documented, accepted limit (the
 * durable answer is keying the estate on a stable user id), and what this service does about it is
 * to stop CLAIMING an erasure it did not perform — see {@code PurgeCommandsConsumer}.
 *
 * <p><strong>The mirror of that race, found while reading and deliberately not fixed here.</strong>
 * A rename landing BETWEEN a mark and its closure moves the reserved rows onto the new address
 * while {@code ERASE_USER_CONTENT} still names the old one, so the closure finds nothing and those
 * references stay hidden and unerased. That is not silent: it is exactly the backlog
 * {@link WatchErasureBacklog} counts and alarms on, which is where a stuck obligation is supposed
 * to surface. Both siblings left the same window open for the same reason.
 */
public class RekeyUserItems {

    private final UserItemsRekey rekey;

    public RekeyUserItems(UserItemsRekey rekey) {
        this.rekey = rekey;
    }

    /** Returns how many rows moved — for the log line; nothing branches on it. */
    public int execute(String oldEmail, String newEmail) {
        return rekey.moveTo(oldEmail, newEmail);
    }
}
