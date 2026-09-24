package com.jrobertgardzinski.collections.closure;

/**
 * What the participant did with a command, counts included. Returned rather than
 * logged-and-forgotten so that a caller in any assembly can act on it: over a broker the consumer
 * turns a {@link Reserved} into the confirmation it publishes, in one process the bus can hand it
 * straight back to the orchestrator.
 *
 * <p>Note what is NOT here and is in the other two participants: a port for the confirmation.
 * This service has no outbox — its confirmation is built and sent by the consumer, per record,
 * before the batch commits — so there is nothing the participant must do INSIDE a unit of work
 * with the mark, and a returned value says everything.
 */
public sealed interface ClosureOutcome {

    /**
     * The reversible step is done: this many of the leaver's references are out of their lists and
     * none of them is destroyed. A count of zero is a real answer and still gets confirmed — see
     * the participant for why reporting beats refusing.
     */
    record Reserved(int references) implements ClosureOutcome {
    }

    /**
     * The closure was carried out. {@code leftBehind} is this axis's own bad news: references
     * saved AFTER the mark, under the address being erased, which no command of this saga may
     * destroy and for which none will ever come.
     */
    record Erased(int erased, int leftBehind) implements ClosureOutcome {
    }

    /** The compensation was carried out: this many references are back in their lists. */
    record Restored(int references) implements ClosureOutcome {
    }

    /**
     * A command this participant has no part in. Not an error: the saga's topic carries every
     * participant's commands, and meeting a name from a newer orchestrator must be survivable.
     */
    record NotOurs(String type) implements ClosureOutcome {
    }

    /**
     * A command that names nobody. Dropped WITHOUT confirming: confirming would tell the
     * orchestrator a deletion happened that never did, and no retry can fix a broken command.
     */
    record Unaddressed(String type) implements ClosureOutcome {
    }
}
