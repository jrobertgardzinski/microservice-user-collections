package com.jrobertgardzinski.collections.closure;

import com.jrobertgardzinski.closure.ClosureCommand;
import com.jrobertgardzinski.closure.ClosureOutcome;
import com.jrobertgardzinski.closure.ClosureParticipant;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.identity.UserId;
import com.jrobertgardzinski.observation.Observations;

/**
 * The collections service's side of the account-closure saga. MARK hides; ERASE destroys what the
 * mark reserved; RESTORE compensates. Only MARK is confirmed — by the consumer, from the returned
 * outcome — and all three are idempotent.
 *
 * <p>Hence {@link ClosureParticipant} and not {@code AtomicClosureParticipant}: this service has no
 * outbox, so it has no confirmation of its own to commit together with the mark. The other
 * difference from the two participants it shares that skeleton with is that a saved reference is a
 * pointer at somebody else's content, so the command's purge rule is never read here: there is no
 * author to anonymise and no popularity of its own to weigh.
 */
public final class CollectionsClosureParticipant extends ClosureParticipant {

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

    @Override
    protected int reserve(String sagaId, UserId leaver) {
        return markForErasure.execute(leaver);
    }

    @Override
    protected void marked(String sagaId, int rows) {
        log.info("marked {} collection refs of one leaver for erasure (saga {})", rows, sagaId);
    }

    /** Destroys only what the mark reserved; a reference saved after the mark is counted and left standing. */
    @Override
    protected ClosureOutcome erase(ClosureCommand command, UserId leaver) {
        PurgeUserItems.Closure closure = purgeUserItems.execute(leaver);
        String sagaId = command.sagaId();
        log.info("erased {} reserved collection refs on the saga's closure (saga {})",
                closure.erased(), sagaId);
        if (closure.leftBehind() > 0) {
            observations.record(new Observation.ErasureResidue(closure.leftBehind()));
            log.warn("the closure of saga {} left {} references standing: saved after the mark, "
                    + "they need removing by hand (collections_erasure_residue_total)",
                    sagaId, closure.leftBehind());
        }
        return new ClosureOutcome.Erased(closure.erased(), closure.leftBehind());
    }

    @Override
    protected ClosureOutcome restore(String sagaId, UserId leaver) {
        int restored = restoreUserItems.execute(leaver);
        log.info("restored {} collection refs: the saga compensated (saga {})", restored, sagaId);
        return new ClosureOutcome.Restored(restored);
    }

    @Override
    protected void reservedNothing() {
        observations.record(new Observation.PurgeReservedNothing());
    }
}
