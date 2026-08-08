package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.infrastructure.JdbcCollectionStore;
import com.jrobertgardzinski.collections.infrastructure.JdbcItemErasure;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The JDBC adapter against a real database: H2 in PostgreSQL mode, migrated by Flyway. */
class JdbcCollectionStoreTest {

    private JdbcCollectionStore store;
    private JdbcItemErasure erasure;

    @BeforeEach
    void migrateFreshDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test_" + UUID.randomUUID() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        HikariDataSource dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionStore(dataSource);
        erasure = new JdbcItemErasure(dataSource);
    }

    @Test
    void save_is_idempotent() {
        ItemRef meme = new ItemRef("meme", "42");
        assertTrue(store.add("alice", "favourites", meme), "first save is new");
        assertFalse(store.add("alice", "favourites", meme), "second save is a no-op");
        assertEquals(1, store.list("alice", "favourites").size());
    }

    @Test
    void lists_newest_first() {
        store.add("alice", "favourites", new ItemRef("meme", "1"));
        store.add("alice", "favourites", new ItemRef("meme", "2"));
        assertEquals(List.of(new ItemRef("meme", "2"), new ItemRef("meme", "1")),
                store.list("alice", "favourites"));
    }

    @Test
    void collections_are_scoped_per_user_and_name() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        assertTrue(store.list("bob", "favourites").isEmpty());
        assertTrue(store.list("alice", "watchlist").isEmpty());
    }

    @Test
    void remove_takes_it_out_and_is_idempotent() {
        ItemRef meme = new ItemRef("meme", "42");
        store.add("alice", "favourites", meme);
        assertTrue(store.remove("alice", "favourites", meme));
        assertFalse(store.remove("alice", "favourites", meme), "removing again is a no-op");
        assertTrue(store.list("alice", "favourites").isEmpty());
    }

    @Test
    void the_mark_empties_every_list_of_the_leaver_and_deletes_nothing() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        store.add("alice", "watchlist", new ItemRef("comment", "7"));
        store.add("bob", "favourites", new ItemRef("meme", "99"));

        assertEquals(2, mark().execute("alice"));

        assertTrue(store.list("alice", "favourites").isEmpty(), "the view hides a marked row");
        assertTrue(store.list("alice", "watchlist").isEmpty(), "in EVERY collection, not just one");
        assertEquals(2, erasure.pendingOf("alice").size(),
                "and both rows are still there — reserved, not deleted");
        assertEquals(1, store.list("bob", "favourites").size(), "bob is untouched");
    }

    @Test
    void the_compensation_puts_the_lists_back_exactly_as_they_were() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        store.add("alice", "favourites", new ItemRef("meme", "43"));
        mark().execute("alice");

        assertEquals(2, new RestoreUserItems(erasure).execute("alice"));

        assertEquals(List.of(new ItemRef("meme", "43"), new ItemRef("meme", "42")),
                store.list("alice", "favourites"),
                "same refs, same newest-first order: the mark touched nothing but the status");
        assertEquals(List.of(), erasure.pendingOf("alice"));
    }

    @Test
    void only_the_closure_deletes_and_only_what_the_mark_reserved() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        mark().execute("alice");
        // saved AFTER the mark: it belongs to no saga, so the closure is not its business
        store.add("alice", "favourites", new ItemRef("meme", "43"));

        assertEquals(1, new PurgeUserItems(erasure).execute("alice"));

        assertEquals(List.of(new ItemRef("meme", "43")), store.list("alice", "favourites"),
                "the later save survives the closure of a saga that never reserved it");
        assertEquals(0, new PurgeUserItems(erasure).execute("alice"),
                "and the closure is idempotent: the second delivery finds nothing reserved");
    }

    @Test
    void a_second_mark_keeps_the_first_instant_so_a_redelivery_cannot_rejuvenate_the_backlog() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        Instant firstDelivery = Instant.parse("2026-08-08T10:00:00Z");
        new MarkUserItemsForErasure(erasure, Clock.fixed(firstDelivery, ZoneOffset.UTC))
                .execute("alice");

        // Kafka redelivers an hour later — the same command, a different clock
        Instant redelivery = firstDelivery.plus(Duration.ofHours(1));
        assertEquals(0, new MarkUserItemsForErasure(erasure, Clock.fixed(redelivery, ZoneOffset.UTC))
                .execute("alice"), "nothing left to mark");

        assertEquals(firstDelivery,
                erasure.pendingOf("alice").getFirst().markedForErasureAt(),
                "the ORIGINAL instant stands: the backlog alarm measures how long this obligation"
                        + " has been owed, and a redelivery must not make an old one look fresh");
        assertEquals(1, erasure.pendingSince(redelivery).size(),
                "so the reaper's query still finds it overdue");
    }

    private MarkUserItemsForErasure mark() {
        return new MarkUserItemsForErasure(erasure, Clock.systemUTC());
    }
}
