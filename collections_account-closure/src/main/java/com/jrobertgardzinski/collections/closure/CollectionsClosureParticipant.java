package com.jrobertgardzinski.collections.closure;

import com.jrobertgardzinski.closure.ClosureCommand;
import com.jrobertgardzinski.closure.ClosureMessages;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The collections service's side of the account-closure saga: the third participant the
 * orchestrator waits for, in TWO phases like its siblings.
 *
 * <ul>
 *   <li>{@code PURGE_USER_CONTENT} — the reversible step: the leaver's saved references are MARKED
 *       ({@link MarkUserItemsForErasure}), which takes them out of every list and destroys
 *       nothing. This is the only command that is confirmed.</li>
 *   <li>{@code ERASE_USER_CONTENT} — the closure: everybody confirmed, the case cannot fail any
 *       more, and {@link PurgeUserItems} destroys what the mark reserved — only that.</li>
 *   <li>{@code RESTORE_USER_CONTENT} — the compensation: a sibling participant failed, so the
 *       marks come off ({@link RestoreUserItems}) and the lists are whole again.</li>
 * </ul>
 *
 * <p>All three are idempotent, so at-least-once delivery needs no extra dedup. The closure and the
 * compensation are not confirmed: they are the orchestrator ENDING the case, and answering would
 * tell it something it has already decided.
 *
 * <p><strong>Two things are this axis's alone</strong>, and both are why reading it beside its
 * siblings is worth the trouble. It carries NO purge rule: a saved reference is a pointer, there
 * is nothing about it to anonymise or keep for its popularity, so the leaver's conditions have no
 * meaning here and the command's policy is never read. And its closure can come up SHORT — see
 * {@link ClosureOutcome.Erased#leftBehind()}.
 *
 * <p><strong>Why this is a module of its own and not a class in the consumer.</strong> Everything
 * above is true whether the commands arrive over Kafka or as a method call in a single process.
 * Nothing in here knows which it is, so both assemblies run the SAME decisions — and the flow can
 * be read, and tested, before anyone picks one.
 */
public final class CollectionsClosureParticipant {

    private static final Logger LOG = LoggerFactory.getLogger(CollectionsClosureParticipant.class);

    /** The reversible mark; its confirmation is what the orchestrator's quorum counts. */
    public static final String MARK = ClosureMessages.PURGE_USER_CONTENT;
    /** The closure: past this command the saga has nothing left to compensate with. */
    public static final String ERASE = ClosureMessages.ERASE_USER_CONTENT;
    /** The compensation: the marks come off and the references are back in their lists. */
    public static final String RESTORE = ClosureMessages.RESTORE_USER_CONTENT;

    private final MarkUserItemsForErasure markForErasure;
    private final RestoreUserItems restoreUserItems;
    private final PurgeUserItems purgeUserItems;
    private final Observations<Observation> observations;

    public CollectionsClosureParticipant(MarkUserItemsForErasure markForErasure,
                                         RestoreUserItems restoreUserItems,
                                         PurgeUserItems purgeUserItems,
                                         Observations<Observation> observations) {
        this.markForErasure = markForErasure;
        this.restoreUserItems = restoreUserItems;
        this.purgeUserItems = purgeUserItems;
        this.observations = observations;
    }

    /** What this service does about one command of a closing account. */
    public ClosureOutcome handle(ClosureCommand command) {
        String type = command.type();
        if (!MARK.equals(type) && !ERASE.equals(type) && !RESTORE.equals(type)) {
            return new ClosureOutcome.NotOurs(type);
        }
        String sagaId = command.sagaId();
        if (!command.isAddressed()) {
            // a command with nobody to act on: retrying can't fix it, and confirming would tell
            // the orchestrator a deletion happened that never did — so drop it unconfirmed
            LOG.warn("dropping {} without an email (saga {})", type, sagaId);
            return new ClosureOutcome.Unaddressed(type);
        }
        // the saga id identifies the run in logs; the e-mail is PII and stays out of INFO lines
        String email = command.email();
        return switch (type) {
            case ERASE -> erase(sagaId, email);
            case RESTORE -> {
                int restored = restoreUserItems.execute(email);
                LOG.info("restored {} collection refs: the saga compensated (saga {})",
                        restored, sagaId);
                yield new ClosureOutcome.Restored(restored);
            }
            case MARK -> mark(sagaId, email);
            default -> throw new IllegalStateException("unreachable: " + type);
        };
    }

    /**
     * The closure. Only this destroys anything, and only what the mark reserved.
     *
     * <p>It can come up short, and that is this axis's own bad news. A reference saved AFTER the
     * mark — by a token this service's offline gate still accepted, because an offline gate is
     * blind to a revocation until the token expires — is not reserved, so no command of this saga
     * may destroy it and none will ever come. It is counted, alarmed on and left standing rather
     * than silently swept, because sweeping it would mean destroying rows this saga never
     * reserved.
     */
    private ClosureOutcome erase(String sagaId, String email) {
        PurgeUserItems.Closure closure = purgeUserItems.execute(email);
        LOG.info("erased {} reserved collection refs on the saga's closure (saga {})",
                closure.erased(), sagaId);
        if (closure.leftBehind() > 0) {
            observations.record(new Observation.ErasureResidue(closure.leftBehind()));
            LOG.warn("the closure of saga {} left {} references standing under the address it"
                    + " erased: they were saved after the mark, by a token this offline gate"
                    + " still accepted, so no command of this saga may destroy them and none"
                    + " will ever come. They need removing by hand —"
                    + " collections_erasure_residue_total is the count", sagaId,
                    closure.leftBehind());
        }
        return new ClosureOutcome.Erased(closure.erased(), closure.leftBehind());
    }

    /**
     * The reversible step — and the count it reserved, which the caller puts on the confirmation.
     *
     * <p>A zero is REPORTED, not refused, and deliberately: withholding the confirmation would
     * fail the deletion of every member who never saved anything — the common case — to catch the
     * rarer one. "I hold nothing of theirs" and "their rows are under the address they had
     * yesterday and the rename has not reached me yet" are the same observation from in here, and
     * the address on the command is all there is to go on. So the participant stops asserting and
     * starts reporting: the saga still completes, and the zero is visible in the log, on the wire
     * and on a counter an alert can bind to.
     */
    private ClosureOutcome mark(String sagaId, String email) {
        int reserved = markForErasure.execute(email);
        LOG.info("marked {} collection refs of one leaver for erasure (saga {})", reserved, sagaId);
        if (reserved == 0) {
            observations.record(new Observation.PurgeReservedNothing());
            LOG.warn("confirming a purge that reserved NOTHING (saga {}): either this member never"
                    + " saved anything, or their references are still keyed by an address they have"
                    + " changed and the rename has not been consumed yet", sagaId);
        }
        return new ClosureOutcome.Reserved(reserved);
    }
}
