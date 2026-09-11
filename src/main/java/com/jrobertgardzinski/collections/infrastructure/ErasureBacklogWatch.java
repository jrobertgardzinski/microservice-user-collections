package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.WatchErasureBacklog;
import com.jrobertgardzinski.collections.domain.Observation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The clock behind {@link WatchErasureBacklog}, and nothing else — the Helidon twin of the
 * {@code @Scheduled} method the two Spring participants get for free.
 *
 * <p>What it used to be: the query, the threshold, the decision and the gauge in one class among
 * the adapters. What is left is a call and the handling of a store that will not answer.
 */
public final class ErasureBacklogWatch {

    private static final Logger LOG = LoggerFactory.getLogger(ErasureBacklogWatch.class);

    private final WatchErasureBacklog watchBacklog;

    public ErasureBacklogWatch(WatchErasureBacklog watchBacklog) {
        this.watchBacklog = watchBacklog;
    }

    /** One pass. Returns how many marks are overdue, or -1 when the register could not be read. */
    public int check() {
        Observation.ErasureBacklog backlog;
        try {
            backlog = watchBacklog.execute();
        } catch (RuntimeException unreadable) {
            // a register we cannot read is loud, never fatal — and nothing is stated, so the gauge
            // KEEPS its last value: reporting zero would turn a failed read into "the backlog is
            // clear", the exact reassurance this alarm exists to withhold
            LOG.error("could not read the erasure backlog — the gauge keeps its last value",
                    unreadable);
            return -1;
        }
        if (backlog.marked() == 0) {
            return 0;   // the normal case: sagas close within minutes
        }
        LOG.warn("{} saved reference(s) have been marked for erasure for longer than the tolerance"
                        + " — the oldest for {}. Their account-deletion saga never sent its closure"
                        + " command, so these rows are hidden but NOT erased. Nothing here will"
                        + " delete them on a timer: re-drive the saga from microservice-offboarding,"
                        + " or compensate it",
                backlog.marked(), backlog.oldest());
        return backlog.marked();
    }
}
