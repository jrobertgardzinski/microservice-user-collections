package com.jrobertgardzinski.collections.closure;

import java.util.Optional;
import com.jrobertgardzinski.identity.UserId;
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
 * The collections service's side of the account-closure saga. MARK hides; ERASE destroys what the
 * mark reserved; RESTORE compensates. Only MARK is confirmed (by the consumer, from the returned
 * outcome), and all three are idempotent. A saved reference is a pointer, so the command's purge
 * rule is never read here.
 */
public final class CollectionsClosureParticipant {

    private static final Logger LOG = LoggerFactory.getLogger(CollectionsClosureParticipant.class);

    public static final String MARK = ClosureMessages.PURGE_USER_CONTENT;
    public static final String ERASE = ClosureMessages.ERASE_USER_CONTENT;
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

    public ClosureOutcome handle(ClosureCommand command) {
        String type = command.type();
        if (!MARK.equals(type) && !ERASE.equals(type) && !RESTORE.equals(type)) {
            return new ClosureOutcome.NotOurs(type);
        }
        String sagaId = command.sagaId();
        if (!command.isAddressed()) {
            // confirming would advance the saga on a deletion that never happened
            LOG.warn("dropping {} without an email (saga {})", type, sagaId);
            return new ClosureOutcome.Unaddressed(type);
        }
        String email = command.email();   // PII: never logged
        Optional<UserId> leaver = command.userId();
        return switch (type) {
            case ERASE -> erase(sagaId, email, leaver);
            case RESTORE -> {
                int restored = restoreUserItems.execute(email, leaver);
                LOG.info("restored {} collection refs: the saga compensated (saga {})", restored, sagaId);
                yield new ClosureOutcome.Restored(restored);
            }
            case MARK -> mark(sagaId, email, leaver);
            default -> throw new IllegalStateException("unreachable: " + type);
        };
    }

    /** Destroys only what the mark reserved; a reference saved after the mark is counted and left standing. */
    private ClosureOutcome erase(String sagaId, String email, Optional<UserId> leaver) {
        PurgeUserItems.Closure closure = purgeUserItems.execute(email, leaver);
        LOG.info("erased {} reserved collection refs on the saga's closure (saga {})", closure.erased(), sagaId);
        if (closure.leftBehind() > 0) {
            observations.record(new Observation.ErasureResidue(closure.leftBehind()));
            LOG.warn("the closure of saga {} left {} references standing: saved after the mark, "
                    + "they need removing by hand (collections_erasure_residue_total)",
                    sagaId, closure.leftBehind());
        }
        return new ClosureOutcome.Erased(closure.erased(), closure.leftBehind());
    }

    private ClosureOutcome mark(String sagaId, String email, Optional<UserId> leaver) {
        int reserved = markForErasure.execute(email, leaver);
        LOG.info("marked {} collection refs of one leaver for erasure (saga {})", reserved, sagaId);
        if (reserved == 0) {
            // "nothing of theirs" and "rows still under their old address" look the same from here
            observations.record(new Observation.PurgeReservedNothing());
            LOG.warn("confirming a purge that reserved NOTHING (saga {})", sagaId);
        }
        return new ClosureOutcome.Reserved(reserved);
    }
}
