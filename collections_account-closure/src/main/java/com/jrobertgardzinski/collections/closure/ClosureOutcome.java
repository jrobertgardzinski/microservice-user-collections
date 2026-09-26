package com.jrobertgardzinski.collections.closure;

/**
 * What the participant did with a command. Unlike the other two participants there is no
 * confirmation port here: this service has no outbox, so the consumer builds the confirmation
 * from the returned value.
 */
public sealed interface ClosureOutcome {

    /** Marked; zero is a real answer. */
    record Reserved(int references) implements ClosureOutcome {
    }

    /** {@code leftBehind}: references saved after the mark, which this saga may not destroy. */
    record Erased(int erased, int leftBehind) implements ClosureOutcome {
    }

    record Restored(int references) implements ClosureOutcome {
    }

    /** The saga's topic carries every participant's commands; an unknown name is not an error. */
    record NotOurs(String type) implements ClosureOutcome {
    }

    /** Dropped WITHOUT confirming: nothing was deleted. */
    record Unaddressed(String type) implements ClosureOutcome {
    }
}
