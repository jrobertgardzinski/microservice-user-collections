package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Where the account-deletion saga's own closure and the deletion cascade can meet on the SAME row
 * in this service too — a leaver who had saved their OWN meme sees {@link PurgeUserItems} (the
 * saga's closure, addressed by {@code user_email}) and {@link PurgeDeletedItem} (the cascade off
 * {@code MEME_DELETED}, addressed by {@code item_type}/{@code item_id}) both reach for the same
 * {@code collection_items} row — forced onto the real database, the same idea as
 * {@code MarkedCommentRaceTest} in microservice-comments.
 *
 * <p><strong>Why this test does not chase a deadlock the way that one does.</strong> Checked in
 * {@code Main.java}: neither {@link PurgeUserItems} nor {@link PurgeDeletedItem} is wrapped in a
 * transaction spanning more than one statement — {@link com.jrobertgardzinski.collections.application.ItemErasure#eraseMarked}
 * and {@link com.jrobertgardzinski.collections.application.ItemReferences#purge} are each ONE
 * {@code DELETE}, auto-committed on its own. A single statement cannot be paused between "locked
 * row A" and "about to lock row B" from outside — there is no seam a test can hook, unlike
 * {@code PurgeUserComments}, which visibly runs {@code purgeVoter} then {@code purgeComment} as two
 * separate steps of ONE transaction. So there is no lock-order inversion to force here: what needs
 * proving instead is the simpler claim both ports' javadocs make on faith — that a row the OTHER
 * mechanism got to first is found already gone, not half-deleted or fought over.
 *
 * <p>Proven the same way {@code PostgresDialectTest} proves its own races: a raw connection holds
 * the row uncommitted, the REAL use case is run concurrently and observed to block on it
 * ({@link #awaitALockWaiter()}), the row is released, and the real call is asserted to finish
 * cleanly having found nothing left to do on it — deterministic, not a timing gamble.
 */
@Epic("Saga")
@Feature("Deletion cascade")
@Story("The saga's closure and the item-deleted cascade race on the same saved reference")
@Testcontainers(disabledWithoutDocker = true)
class MarkedItemRaceTest {

    private static final String LEAVER = "leaver@example.com";
    private static final String FAN = "fan@example.com";
    private static final String MEME = "own-meme";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18-alpine");

    static HikariDataSource dataSource;
    static JdbcCollectionRepository store;
    static JdbcItemErasure erasure;

    @BeforeAll
    static void migrateAndConnect() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        // the probe needs its own connection alongside whatever the real use case borrows
        config.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionRepository(dataSource);
        erasure = new JdbcItemErasure(dataSource);
    }

    @AfterAll
    static void closePool() {
        dataSource.close();
    }

    @BeforeEach
    void freshFixture() {
        // leaver's own meme, saved by its own author AND by a fan — the row a running saga has
        // reserved (marked_for_erasure_at set) is exactly the row the cascade would destroy anyway,
        // because the cascade's WHERE clause never looks at status (see JdbcCollectionRepository#purge)
        store.add(LEAVER, "favourites", new com.jrobertgardzinski.collections.domain.ItemRef("meme", MEME));
        store.add(FAN, "favourites", new com.jrobertgardzinski.collections.domain.ItemRef("meme", MEME));
        markLeaversItemForErasure();
    }

    @Test
    @DisplayName("PG18: the cascade's purge blocks behind the saga's in-flight closure, and finds "
            + "nothing left of the leaver's row when it resumes")
    void cascade_blocks_behind_the_saga_then_finds_nothing() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection probe = dataSource.getConnection()) {
            probe.setAutoCommit(false);
            // simulates PurgeUserItems.execute(LEAVER) mid-flight: the exact DELETE eraseMarked
            // issues, held open — the leaver's row is locked but not yet committed gone
            try (PreparedStatement delete = probe.prepareStatement(
                    "DELETE FROM collection_items WHERE user_email = ? AND status = 'PENDING_ERASURE'")) {
                delete.setString(1, LEAVER);
                delete.executeUpdate();
            }

            // the REAL cascade, for real: it must touch both the leaver's row and the fan's
            Future<Integer> cascadeOutcome = pool.submit(
                    () -> new PurgeDeletedItem(store).execute("meme", List.of(MEME)));

            awaitALockWaiter();
            probe.commit();   // the saga's closure "lands": the leaver's row is truly gone now

            // bounded get(): a stuck adapter fails here as a readable TimeoutException, not a hang
            int removed = cascadeOutcome.get(15, TimeUnit.SECONDS);
            assertEquals(1, removed, "only the fan's row was still there to remove — the leaver's "
                    + "had already gone with the saga's own closure");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, countByMeme(), "neither reference to the deleted meme may survive");
    }

    @Test
    @DisplayName("PG18: the saga's closure blocks behind an in-flight cascade purge, and finds "
            + "nothing left to erase when it resumes")
    void saga_blocks_behind_the_cascade_then_finds_nothing() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection probe = dataSource.getConnection()) {
            probe.setAutoCommit(false);
            // simulates PurgeDeletedItem's cascade mid-flight: the exact DELETE
            // ItemReferences#purge issues, held open — it does not filter on status, so it already
            // holds BOTH the leaver's row and the fan's
            try (PreparedStatement delete = probe.prepareStatement(
                    "DELETE FROM collection_items WHERE item_type = 'meme' AND item_id = ?")) {
                delete.setString(1, MEME);
                delete.executeUpdate();
            }

            // the REAL saga closure, for real
            Future<PurgeUserItems.Closure> purgeOutcome = pool.submit(
                    () -> new PurgeUserItems(erasure).execute(LEAVER));

            awaitALockWaiter();
            probe.commit();   // the cascade "lands": both references are truly gone now

            PurgeUserItems.Closure closure = purgeOutcome.get(15, TimeUnit.SECONDS);
            assertEquals(new PurgeUserItems.Closure(0, 0), closure,
                    "the cascade got there first: nothing was left reserved to erase, and nothing "
                            + "of the leaver's is left behind to count either");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, countByMeme(), "neither reference to the deleted meme may survive");
    }

    private static void markLeaversItemForErasure() {
        try (Connection c = dataSource.getConnection();
             PreparedStatement update = c.prepareStatement(
                     "UPDATE collection_items SET status = 'PENDING_ERASURE', "
                             + "marked_for_erasure_at = CURRENT_TIMESTAMP "
                             + "WHERE user_email = ? AND item_type = 'meme' AND item_id = ?")) {
            update.setString(1, LEAVER);
            update.setString(2, MEME);
            update.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("could not mark the fixture's row for erasure", e);
        }
    }

    private static long countByMeme() throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement select = c.prepareStatement(
                     "SELECT COUNT(*) FROM collection_items WHERE item_type = 'meme' AND item_id = ?")) {
            select.setString(1, MEME);
            try (var rs = select.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * Wait (briefly) until some other session is blocked on a lock — the sign that the real use
     * case under test has reached the contended row, so committing the probe now is what actually
     * unblocks it. Same guard as {@code PostgresDialectTest}: the probe counts lock waiters across
     * the whole cluster, so this assumes sequential test execution (the JUnit default).
     */
    private static void awaitALockWaiter() throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        try (Connection c = dataSource.getConnection()) {
            while (System.currentTimeMillis() < deadline) {
                try (PreparedStatement count = c.prepareStatement(
                        "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'");
                     var rs = count.executeQuery()) {
                    rs.next();
                    if (rs.getLong(1) > 0) {
                        return;
                    }
                }
                Thread.sleep(20);
            }
        }
        fail("timed out waiting for the real use case to block on the probe's uncommitted delete");
    }
}
