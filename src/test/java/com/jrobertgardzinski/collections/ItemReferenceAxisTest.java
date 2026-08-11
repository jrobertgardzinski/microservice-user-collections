package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.JdbcCollectionStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The item axis against a real database: H2 in PostgreSQL mode, migrated by Flyway — the same
 * adapter and the same migrations production runs.
 *
 * <p>Two things are pinned here that the in-memory store cannot pin. First the SQL itself: a
 * {@code DELETE ... WHERE item_type = ? AND item_id IN (...)} is exactly the shape that quietly
 * eats a neighbour's rows when half the key is forgotten, and it is the only statement in this
 * service that is not scoped to one user. Second the INDEX: without V2 the same statement is a
 * full scan of everyone's collections, and nothing in a green functional test would ever say so —
 * so the migration's index is asserted from the catalogue, the way an operator would check it.
 */
@Epic("Infrastructure")
@Feature("Collection persistence")
@Story("Item axis and its index")
class ItemReferenceAxisTest {

    private DataSource dataSource;
    private JdbcCollectionStore store;

    @BeforeEach
    void migrateFreshDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:item_" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionStore(dataSource);
    }

    @Test
    void the_migrations_leave_an_index_on_the_item_axis() {
        // the claim, not the spelling: SOME index must lead with (item_type, item_id). Asserting
        // the name alone would pass for an index on the wrong columns, and asserting the columns
        // alone would pass for one that only helps a query nobody runs
        List<String> itemAxisIndexes = indexesLeadingWith("item_type", "item_id");

        assertEquals(1, itemAxisIndexes.size(),
                "exactly one index must answer \"who saved THIS item?\" — found: "
                        + itemAxisIndexes);
        assertTrue(itemAxisIndexes.get(0).contains("item"),
                "and it should say so in its name, for whoever reads an EXPLAIN: "
                        + itemAxisIndexes.get(0));
    }

    @Test
    void the_v1_user_index_survives_the_new_migration() {
        // V2 adds an axis, it does not rewrite V1 — the account-deletion purge still needs its
        // own index, and a migration that "tidied" it away would be invisible until a leaver left.
        // More than one index may lead with user_email (the UNIQUE constraint does too), so the
        // claim is that V1's is among them
        assertTrue(indexesLeadingWith("user_email").contains("idx_collection_items_user"),
                "the user axis must still have V1's own index after V2, found: "
                        + indexesLeadingWith("user_email"));
    }

    @Test
    void purging_an_item_takes_every_users_ref_to_it_and_nothing_else() {
        store.add("alice", "favourites", new ItemRef("meme", "m-1"));
        store.add("alice", "watchlist", new ItemRef("meme", "m-1"));
        store.add("bob", "favourites", new ItemRef("meme", "m-1"));
        store.add("bob", "favourites", new ItemRef("comment", "m-1"));   // same id, other type
        store.add("bob", "favourites", new ItemRef("meme", "m-2"));      // other id, same type

        assertEquals(3, store.purge("meme", List.of("m-1")));

        assertTrue(store.list("alice", "favourites").isEmpty());
        assertTrue(store.list("alice", "watchlist").isEmpty());
        assertEquals(List.of(new ItemRef("meme", "m-2"), new ItemRef("comment", "m-1")),
                store.list("bob", "favourites"),
                "the (comment, m-1) and (meme, m-2) refs are not the deleted meme");
    }

    @Test
    void purging_an_item_twice_removes_nothing_the_second_time() {
        store.add("alice", "favourites", new ItemRef("comment", "c-1"));

        assertEquals(1, store.purge("comment", List.of("c-1")));
        assertEquals(0, store.purge("comment", List.of("c-1")),
                "a redelivered event must cost nothing and raise nothing");
    }

    @Test
    void a_batch_larger_than_one_statements_worth_of_ids_still_purges_all_of_it() {
        // the chunking in the adapter: a busy thread's COMMENTS_DELETED can name more ids than
        // one IN (...) should carry, and the seam between two chunks is where a count goes wrong
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 1_200; i++) {
            String id = "c-" + i;
            ids.add(id);
            store.add("alice", "favourites", new ItemRef("comment", id));
        }
        store.add("alice", "favourites", new ItemRef("meme", "kept"));

        assertEquals(1_200, store.purge("comment", ids),
                "every chunk's rows must be counted, not just the last one's");
        assertEquals(List.of(new ItemRef("meme", "kept")), store.list("alice", "favourites"));
    }

    /** The names of the indexes whose leading columns are exactly these, straight from H2. */
    private List<String> indexesLeadingWith(String... columns) {
        List<String> names = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet indexes = statement.executeQuery(
                     "SELECT index_name, column_name, ordinal_position"
                             + " FROM information_schema.index_columns"
                             + " WHERE table_name = 'collection_items' ORDER BY index_name,"
                             + " ordinal_position")) {
            String current = null;
            List<String> currentColumns = new ArrayList<>();
            while (indexes.next()) {
                String name = indexes.getString("index_name");
                if (!name.equals(current)) {
                    addIfLeadingWith(names, current, currentColumns, columns);
                    current = name;
                    currentColumns = new ArrayList<>();
                }
                currentColumns.add(indexes.getString("column_name").toLowerCase());
            }
            addIfLeadingWith(names, current, currentColumns, columns);
        } catch (Exception unreadableCatalogue) {
            throw new IllegalStateException("could not read the index catalogue",
                    unreadableCatalogue);
        }
        return names;
    }

    private static void addIfLeadingWith(List<String> names, String index, List<String> actual,
                                         String... expectedLeading) {
        if (index == null || actual.size() < expectedLeading.length) {
            return;
        }
        for (int i = 0; i < expectedLeading.length; i++) {
            if (!actual.get(i).equals(expectedLeading[i])) {
                return;
            }
        }
        names.add(index.toLowerCase());
    }
}
