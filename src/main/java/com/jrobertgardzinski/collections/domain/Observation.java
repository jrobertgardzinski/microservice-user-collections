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
}
