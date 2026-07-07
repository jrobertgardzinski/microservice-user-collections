package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.JdbcCollectionStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The JDBC adapter against a real database: H2 in PostgreSQL mode, migrated by Flyway. */
class JdbcCollectionStoreTest {

    private JdbcCollectionStore store;

    @BeforeEach
    void migrateFreshDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test_" + UUID.randomUUID() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        HikariDataSource dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionStore(dataSource);
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
    void purge_removes_all_of_a_users_items_across_collections() {
        store.add("alice", "favourites", new ItemRef("meme", "42"));
        store.add("alice", "watchlist", new ItemRef("comment", "7"));
        store.add("bob", "favourites", new ItemRef("meme", "99"));

        assertEquals(2, store.purgeUser("alice"));
        assertTrue(store.list("alice", "favourites").isEmpty());
        assertTrue(store.list("alice", "watchlist").isEmpty());
        assertEquals(1, store.list("bob", "favourites").size(), "bob is untouched");
    }
}
