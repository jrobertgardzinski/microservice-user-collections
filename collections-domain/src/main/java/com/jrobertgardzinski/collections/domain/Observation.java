package com.jrobertgardzinski.collections.domain;

import java.time.Duration;

/**
 * Something this service has noticed about itself and considers worth saying out loud — a domain
 * probe: the code states a fact in the language of the business, and whatever is watching
 * translates it into its own.
 *
 * <p>Sealed because it is a vocabulary, not an extension point. Every fact here is a sentence about
 * account deletion that <strong>no tool could derive on its own</strong>: an agent knows a method
 * took 40ms, but nothing outside this service can know that a leaver's saved references are sitting
 * marked-but-not-erased, because that is a conclusion drawn from this service's own rules.
 *
 * <p>Timings, memory, thread counts and uptime never appear here — they are properties of a
 * process, readable by anything that can see it, and spelled in whatever vocabulary this year's
 * tool uses. What is in here is what survives replacing that tool.
 */
public sealed interface Observation {

    /**
     * References reserved by a deletion saga whose closure never came: the leaver's list looks
     * empty, the rows are still there. {@code oldest} is how long the oldest such mark has stood —
     * an age, and therefore not personal data, which matters because the owners of these rows are
     * exactly the people this service is trying to forget.
     *
     * <p>Stated on EVERY pass, zero included: the question is "how many obligations am I sitting on
     * right now", so silence would leave a stale answer standing.
     */
    record ErasureBacklog(int marked, Duration oldest) implements Observation {

        public static final ErasureBacklog NONE = new ErasureBacklog(0, Duration.ZERO);
    }

    /**
     * One saga command this service will now never carry out: the retry budget ran out with the
     * database or the broker still unreachable. Whether that is survivable depends on which command
     * it was — a lost mark ends in the orchestrator compensating, a lost closure leaves rows hidden
     * for ever — and the service cannot tell which, so it says what it knows.
     */
    record SagaCommandDropped(String topic) implements Observation {
    }

    /**
     * A deletion saga reserved NOTHING for the person it named, and this service confirmed that it
     * had nothing to reserve. Harmless when the member really never saved anything — and the whole
     * of the address-change defect when they did, under the address they used to have: the rename
     * travels on another topic and can arrive after the purge, so this is the one moment at which
     * "I hold nothing of theirs" and "I have not caught up with their new address" look identical
     * from the inside.
     *
     * <p>Stated because nothing else can state it. A broker dashboard sees a command consumed and a
     * confirmation produced; only this service knows the confirmation was empty, and the shape of
     * the defect is that a rise in this count accompanies deletions that leave references behind.
     */
    record PurgeReservedNothing() implements Observation {
    }

    /**
     * References still standing under the address of somebody this service has just erased: saved
     * after the mark reserved the rest, so the closure — which may only destroy what was reserved —
     * had to leave them, and no later command will ever come for them.
     *
     * <p>They exist because the gate here is offline: a deletion locks signing IN, while an access
     * token already in a tab keeps being accepted until it expires. This service cannot refuse that
     * write, and it cannot safely delete the row afterwards either (the same address may since have
     * been taken by somebody else), so it does the one thing that is honest alone: it says the row
     * is there. Nothing else can — the backlog alarm counts MARKS, and this row carries none.
     */
    record ErasureResidue(int refs) implements Observation {
    }
}
