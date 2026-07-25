package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two probe markers on the monotonic clock: {@code healthy()} (readiness, /health) measures
 * elapsed time since the last COMPLETED cycle, {@code alive()} (liveness, /alive) since the last
 * SCHEDULED iteration — both with {@link System#nanoTime()}, not the wall clock: an NTP step
 * backwards must not fake a 503, one forwards must not mask a real stall. The tests age the
 * markers directly (package-private seam) instead of sleeping out a real stall; the seconds-scale
 * offsets double as a unit check (a millis/nanos mix-up flips the verdicts). Alongside:
 * {@link Main#stallSeconds} must fail fast but READABLY on a broken env value — unparseable,
 * zero and negative alike, naming the variable it refuses.
 */
class PurgeConsumerHealthTest {

    private final CollectionStore store = new CollectionStore() {
        public boolean add(String user, String collection, ItemRef item) { return true; }
        public boolean remove(String user, String collection, ItemRef item) { return false; }
        public List<ItemRef> list(String user, String collection) { return List.of(); }
        public int purgeUser(String user) { return 0; }
    };

    private final PurgeCommandsConsumer consumer =
            new PurgeCommandsConsumer(new PurgeUserItems(store), new ObjectMapper());

    @Test
    void a_fresh_consumer_is_healthy() {
        // the marker is stamped at construction, so a service still warming up is not born ill
        assertTrue(consumer.healthy(Duration.ofSeconds(60)));
    }

    @Test
    void a_marker_older_than_the_tolerance_reports_a_stall() {
        consumer.lastCycleNanos = System.nanoTime() - Duration.ofSeconds(2).toNanos();
        assertFalse(consumer.healthy(Duration.ofSeconds(1)),
                "2s since the last cycle exceeds a 1s tolerance");
    }

    @Test
    void a_marker_within_the_tolerance_stays_healthy() {
        consumer.lastCycleNanos = System.nanoTime() - Duration.ofSeconds(2).toNanos();
        assertTrue(consumer.healthy(Duration.ofSeconds(5)),
                "2s since the last cycle is within a 5s tolerance");
    }

    @Test
    void a_fresh_consumer_is_alive() {
        // the scheduled marker is stamped at construction too — /alive is not born a 503
        assertTrue(consumer.alive(Duration.ofSeconds(120)));
    }

    @Test
    void a_scheduled_marker_older_than_the_tolerance_reports_an_alive_stall() {
        // the thread-is-gone scenario, aged directly: nothing refreshes the marker any more
        consumer.lastScheduledNanos = System.nanoTime() - Duration.ofSeconds(2).toNanos();
        assertFalse(consumer.alive(Duration.ofSeconds(1)),
                "2s since the last scheduled iteration exceeds a 1s tolerance");
    }

    @Test
    void a_stalled_cycle_with_a_fresh_schedule_is_alive_but_not_healthy() {
        // the split the two endpoints exist for: cycles stopped completing (dependency down),
        // yet the loop thread keeps being scheduled — readiness 503, liveness 200
        consumer.lastCycleNanos = System.nanoTime() - Duration.ofSeconds(10).toNanos();
        assertFalse(consumer.healthy(Duration.ofSeconds(1)), "/health must report the stall");
        assertTrue(consumer.alive(Duration.ofSeconds(1)), "/alive must stay green through it");
    }

    @Test
    void an_unparseable_stall_env_names_the_variable_and_the_value() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> Main.stallSeconds("COLLECTIONS_CONSUMER_STALL_SEC", "sixty"));
        assertTrue(failure.getMessage().contains("COLLECTIONS_CONSUMER_STALL_SEC"),
                "the fail-fast message must name the variable to fix");
        assertTrue(failure.getMessage().contains("sixty"),
                "the fail-fast message must quote the value that broke it");
    }

    @Test
    void a_zero_stall_env_refuses_to_start_naming_the_variable_and_the_value() {
        // 0 parses fine but means "every probe reports a stall" — a config mistake, not a wish
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> Main.stallSeconds("COLLECTIONS_CONSUMER_STALL_SEC", "0"));
        assertTrue(failure.getMessage().contains("COLLECTIONS_CONSUMER_STALL_SEC"),
                "the fail-fast message must name the variable to fix");
        assertTrue(failure.getMessage().contains("0"),
                "the fail-fast message must quote the refused value");
    }

    @Test
    void a_negative_stall_env_refuses_to_start_for_either_variable() {
        // the same guard serves both envs; the message must carry whichever name was passed
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> Main.stallSeconds("COLLECTIONS_ALIVE_STALL_SEC", "-5"));
        assertTrue(failure.getMessage().contains("COLLECTIONS_ALIVE_STALL_SEC"),
                "the fail-fast message must name the variable to fix");
        assertTrue(failure.getMessage().contains("-5"),
                "the fail-fast message must quote the refused value");
    }

    @Test
    void the_alive_floor_covers_the_whole_worst_legal_iteration() {
        // The finding this pins. The floor used to be 2 x max(delivery.timeout, api.timeout,
        // max backoff) + a poll + a probe = 83s, and the test recomputed that same formula —
        // which passes no matter how wrong the formula is. A worst legal iteration actually
        // spends its blocks in SEQUENCE, and back then they came to 106s: the "safe minimum" sat
        // BELOW the case it was sold as covering. So the blocks are enumerated here from what
        // one cycle really calls, in order, and the floor must COVER their sum.
        Duration worstIteration = PurgeCommandsConsumer.API_TIMEOUT   // rewind: committed()
                .plus(PurgeCommandsConsumer.PROBE_TIMEOUT)            // the /health honesty probe
                .plus(PurgeCommandsConsumer.POLL_EVERY)               // poll()
                .plus(Database.WORST_BLOCK)                           // the purge's own store work
                .plus(PurgeCommandsConsumer.DELIVERY_TIMEOUT)         // the confirmation send
                .plus(PurgeCommandsConsumer.API_TIMEOUT)              // commitSync()
                .plus(PurgeCommandsConsumer.MAX_BACKOFF);             // the pause before the retry
        assertEquals(worstIteration, Main.WORST_ITERATION,
                "Main must add up the blocks a cycle really spends, in full");
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(worstIteration) > 0,
                "the floor must sit strictly ABOVE the worst legal iteration: alive() compares"
                        + " with <=, and a GC pause on top of an honest worst case must not read"
                        + " dead. Floor " + Main.ALIVE_STALL_FLOOR.toSeconds() + "s vs iteration "
                        + worstIteration.toSeconds() + "s");

        // and the absolute values, spelled out: a silent drift in any constant above (or in the
        // margin) has to break the build with the new number visible, not slide through
        assertEquals(Duration.ofSeconds(146), Main.WORST_ITERATION,
                "20 (committed) + 5 (probe) + 1 (poll) + 40 (database) + 30 (send)"
                        + " + 20 (commit) + 30 (backoff)");
        assertEquals(Duration.ofSeconds(183), Main.ALIVE_STALL_FLOOR, "146s + 25% margin");
        assertEquals(Duration.ofSeconds(183), Main.ALIVE_STALL_FLOOR,
                "and identical to microservice-offboarding's: one saga, one operational story");
    }

    @Test
    void the_code_default_sits_above_the_floor_instead_of_being_corrected_by_it() {
        // a default the floor silently raises is not a default: the javadoc, the k8s manifests
        // and the operator would all be quoting a number the service never uses. 120s stopped
        // being one the moment the floor was computed honestly (183s)
        assertTrue(Main.DEFAULT_ALIVE_STALL.compareTo(Main.ALIVE_STALL_FLOOR) >= 0,
                "COLLECTIONS_ALIVE_STALL_SEC's default (" + Main.DEFAULT_ALIVE_STALL.toSeconds()
                        + "s) must not be below the floor (" + Main.ALIVE_STALL_FLOOR.toSeconds()
                        + "s)");
        assertEquals(Main.DEFAULT_ALIVE_STALL,
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Main.DEFAULT_ALIVE_STALL),
                "and it must therefore pass through the floor untouched");
    }

    @Test
    void the_database_clocks_are_a_term_of_the_floor_not_an_unbounded_wait() {
        // the last unguarded block in the loop: pgjdbc leaves socketTimeout at 0 = forever, so a
        // SILENT database (partition, frozen node, a purge DELETE behind somebody else's lock)
        // used to wedge the loop thread with no bound — no beat, /alive 503, restart, same lock
        assertTrue(Database.SOCKET_TIMEOUT.toSeconds() > 0,
                "an unbounded socket read is an unbounded liveness gap");
        assertTrue(Database.STATEMENT_TIMEOUT.compareTo(Database.SOCKET_TIMEOUT) < 0,
                "the server-side cancel must fire BEFORE the client abandons the socket, or the"
                        + " lock waiter outlives the connection that was waiting on it");
        assertEquals(Database.CONNECTION_TIMEOUT.plus(Database.SOCKET_TIMEOUT),
                Database.WORST_BLOCK,
                "the two chain in the worst case: a near-full wait for a connection, then a"
                        + " silent read on it");
        assertTrue(Main.WORST_ITERATION.compareTo(Database.WORST_BLOCK) > 0,
                "and the floor's arithmetic must actually carry that block");
    }

    @Test
    void the_batch_is_capped_so_a_drained_backlog_cannot_outrun_the_poll_interval() {
        // this loop confirms each record SYNCHRONOUSLY (send().get() per record), so a batch
        // costs O(N) round trips. Kafka's default of 500 meant the first poll after an outage
        // could hand back a backlog whose handling outlasts max.poll.interval.ms (300s) — the
        // group drops the member, commitSync() fails, the retry re-handles the SAME oversized
        // batch, and the loop livelocks on a rebalance it keeps causing
        assertEquals(String.valueOf(PurgeCommandsConsumer.MAX_POLL_RECORDS),
                PurgeCommandsConsumer.consumerProps("localhost:9092")
                        .getProperty("max.poll.records"),
                "max.poll.records must be set explicitly, never left at Kafka's 500");
        assertTrue(PurgeCommandsConsumer.MAX_POLL_RECORDS < 500,
                "the whole point is to sit below the default");
        assertTrue(PurgeCommandsConsumer.MAX_POLL_RECORDS > 0,
                "and a batch of nothing would starve the saga");
    }

    @Test
    void the_consumers_own_blocking_clock_is_explicit_and_inside_the_alive_floor() {
        // the finding this pins: only the PRODUCER's clocks used to be set, so commitSync(),
        // the rewind's committed() lookup and the readiness probe each still waited Kafka's
        // 60s default.api.timeout.ms on a dead broker — two of those in one iteration outlast
        // the whole /alive tolerance, and the outage reads as a wedged thread
        assertEquals(String.valueOf(PurgeCommandsConsumer.API_TIMEOUT.toMillis()),
                PurgeCommandsConsumer.consumerProps("localhost:9092")
                        .getProperty("default.api.timeout.ms"),
                "the consumer's api timeout must be set explicitly, never left at Kafka's 60s");
        assertEquals(String.valueOf(PurgeCommandsConsumer.REQUEST_TIMEOUT.toMillis()),
                PurgeCommandsConsumer.consumerProps("localhost:9092")
                        .getProperty("request.timeout.ms"));
        assertTrue(PurgeCommandsConsumer.REQUEST_TIMEOUT
                        .compareTo(PurgeCommandsConsumer.API_TIMEOUT) < 0,
                "one in-flight request must fit inside one API call");
        assertTrue(Main.ALIVE_STALL_FLOOR
                        .compareTo(PurgeCommandsConsumer.API_TIMEOUT.multipliedBy(2)) > 0,
                "two consumer API waits in one iteration must still fit inside the tolerance");
    }

    @Test
    void an_alive_stall_below_the_derived_floor_is_floored() {
        // below the floor a broker outage (a send legitimately blocked up to delivery.timeout,
        // then the backoff) would read as a dead thread and restart the pod for nothing
        assertEquals(Main.ALIVE_STALL_FLOOR,
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Duration.ofSeconds(30)));
        assertEquals(Main.ALIVE_STALL_FLOOR, Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC",
                Main.ALIVE_STALL_FLOOR.minusSeconds(1)));
    }

    @Test
    void an_alive_stall_at_or_above_the_derived_floor_is_kept() {
        assertEquals(Duration.ofSeconds(300),
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Duration.ofSeconds(300)));
        assertEquals(Main.ALIVE_STALL_FLOOR,
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Main.ALIVE_STALL_FLOOR));
    }
}
