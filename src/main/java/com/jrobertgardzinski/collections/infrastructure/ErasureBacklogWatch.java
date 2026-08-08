package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.domain.SavedItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watches the erasure backlog — and does nothing about it on purpose. The Helidon twin of
 * microservice-memes' {@code StuckErasureWatch} and microservice-comments' one; the argument is
 * identical, which is why it is worth having in all three rather than in the two that happen to run
 * on Spring.
 *
 * <p>A reference is marked {@code PENDING_ERASURE} by the saga's first, reversible step, and only
 * the orchestrator's closure turns that mark into a delete. A mark standing longer than any saga
 * can legitimately last therefore means the closure never arrived — the retry budget was spent, or
 * the record was dropped. The leaver's list looks empty, which is what they asked for, but the rows
 * are still there, which is not what the GDPR asked for, and nobody would ever find out: nothing is
 * broken, nothing throws, a query simply returns fewer rows for ever.
 *
 * <p><strong>Why it does not just erase them.</strong> Deleting on the passage of time means
 * guessing what the orchestrator decided, and the guess is wrong exactly where it is expensive: a
 * saga stuck for an hour because a sibling is down is a saga that may still COMPENSATE, and content
 * erased on a timer cannot come back. So the backlog becomes an alarm an operator acts on (re-drive
 * the closure, or compensate it), never an expiry.
 *
 * <p>{@code collections_erasure_backlog} is a GAUGE and not a counter: the question is "how many
 * obligations am I sitting on right now", and it must fall back to zero by itself the moment the
 * closure finally lands.
 */
public final class ErasureBacklogWatch {

    private static final Logger LOG = LoggerFactory.getLogger(ErasureBacklogWatch.class);

    /**
     * How old a mark has to be before it is evidence of a lost command rather than of a saga still
     * running. Derived, not guessed, and the same 30 minutes the two siblings use: the orchestrator
     * gives a purge 120s and re-commands it 3 times, so a case is decided within about eight
     * minutes; thirty leaves room for a slow deployment and still raises a genuinely lost closure
     * within the same shift.
     */
    public static final Duration DEFAULT_STUCK_AFTER = Duration.ofMinutes(30);

    /**
     * Static for the same reason the dropped-record counter is: this service exports metrics as a
     * plain function of the process, with no registry to hold instance state.
     */
    private static final AtomicLong BACKLOG = new AtomicLong();

    private final ItemErasure erasure;
    private final Clock clock;
    private final Duration stuckAfter;

    public ErasureBacklogWatch(ItemErasure erasure, Clock clock, Duration stuckAfter) {
        this.erasure = erasure;
        this.clock = clock;
        this.stuckAfter = stuckAfter;
    }

    /** What {@link MetricsEndpoint} exports; zero until the first pass has run. */
    static long backlog() {
        return BACKLOG.get();
    }

    /** One pass. Returns how many marks are overdue, so a test needs no gauge to read. */
    public int check() {
        List<SavedItem> stuck;
        try {
            stuck = erasure.pendingSince(clock.instant().minus(stuckAfter));
        } catch (RuntimeException unreadable) {
            // a register we cannot read is loud, never fatal — and the gauge KEEPS its last value:
            // reporting zero would turn a failed read into "the backlog is clear"
            LOG.error("could not read the erasure backlog — the gauge keeps its last value",
                    unreadable);
            return -1;
        }
        BACKLOG.set(stuck.size());
        if (stuck.isEmpty()) {
            return 0;   // the normal case: sagas close within minutes
        }
        // the OLDEST mark is what an operator needs to judge severity, and an age is not PII — the
        // owners of these rows are exactly the people this service is trying to forget
        Instant oldest = stuck.stream().map(SavedItem::markedForErasureAt)
                .min(Instant::compareTo).orElseThrow();
        LOG.warn("{} saved reference(s) have been marked for erasure for longer than {} — the"
                        + " oldest for {}. Their account-deletion saga never sent its closure"
                        + " command, so these rows are hidden but NOT erased. Nothing here will"
                        + " delete them on a timer: re-drive the saga from microservice-offboarding,"
                        + " or compensate it",
                stuck.size(), stuckAfter, Duration.between(oldest, clock.instant()));
        return stuck.size();
    }
}
