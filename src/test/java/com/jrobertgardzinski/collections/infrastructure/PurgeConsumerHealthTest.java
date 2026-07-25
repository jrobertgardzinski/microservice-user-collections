package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The liveness marker behind /health, on the monotonic clock: {@code healthy()} measures elapsed
 * time since the last completed cycle with {@link System#nanoTime()}, not the wall clock — an NTP
 * step backwards must not fake a 503, one forwards must not mask a real stall. The tests age the
 * marker directly (package-private seam) instead of sleeping out a real stall; the seconds-scale
 * offsets double as a unit check (a millis/nanos mix-up flips both verdicts). Alongside:
 * {@link Main#stallSeconds} must fail fast but READABLY on a broken env value.
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
    void an_unparseable_stall_env_names_the_variable_and_the_value() {
        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> Main.stallSeconds("sixty"));
        assertTrue(failure.getMessage().contains("COLLECTIONS_CONSUMER_STALL_SEC"),
                "the fail-fast message must name the variable to fix");
        assertTrue(failure.getMessage().contains("sixty"),
                "the fail-fast message must quote the value that broke it");
    }
}
