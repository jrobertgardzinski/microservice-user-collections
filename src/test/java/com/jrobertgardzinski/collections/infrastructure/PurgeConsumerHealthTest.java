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
    void the_alive_floor_is_derived_from_the_loop_clocks_not_a_magic_number() {
        // 2 x max(delivery.timeout, max backoff) + a poll + a broker probe — recomputed here
        // from the same constants, so a drift on either side breaks the build
        Duration longestBlock =
                PurgeCommandsConsumer.DELIVERY_TIMEOUT.compareTo(PurgeCommandsConsumer.MAX_BACKOFF) >= 0
                        ? PurgeCommandsConsumer.DELIVERY_TIMEOUT : PurgeCommandsConsumer.MAX_BACKOFF;
        assertEquals(longestBlock.multipliedBy(2)
                        .plus(PurgeCommandsConsumer.POLL_EVERY)
                        .plus(PurgeCommandsConsumer.PROBE_TIMEOUT),
                Main.ALIVE_STALL_FLOOR);
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(Duration.ofSeconds(120)) < 0,
                "the documented 120s default must sit above the floor");
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
        assertEquals(Duration.ofSeconds(120),
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Duration.ofSeconds(120)));
        assertEquals(Main.ALIVE_STALL_FLOOR,
                Main.flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC", Main.ALIVE_STALL_FLOOR));
    }
}
