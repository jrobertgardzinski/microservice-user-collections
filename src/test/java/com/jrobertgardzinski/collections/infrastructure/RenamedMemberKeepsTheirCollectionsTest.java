package com.jrobertgardzinski.collections.infrastructure;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The defect, in the two ways it showed: a member who changed their e-mail address read back an
 * EMPTY favourites list, and deleting that account confirmed an erasure that had matched no rows.
 *
 * <p>Against a real database on purpose — H2 in PostgreSQL mode, migrated by Flyway, the same shape
 * {@code JdbcCollectionStoreTest} uses. The whole fix is one column and the UNIQUE constraint it
 * takes part in, and an in-memory store would agree with any SQL at all.
 */
@Epic("Saga")
@Feature("A member's address can move")
class RenamedMemberKeepsTheirCollectionsTest {

    private static final String OLD = "alice@old.example.com";
    private static final String NEW = "alice@new.example.com";
    private static final ItemRef MEME = new ItemRef("meme", "42");
    private static final ItemRef COMMENT = new ItemRef("comment", "7");

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Observation> stated = new ArrayList<>();
    private final ListAppender<ILoggingEvent> logLines = new ListAppender<>();

    private JdbcCollectionStore store;
    private JdbcItemErasure erasure;
    private SecurityEventsConsumer renames;
    private PurgeCommandsConsumer purges;

    @BeforeEach
    void migrateFreshDatabaseAndTapTheLog() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:rekey_" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        HikariDataSource dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        store = new JdbcCollectionStore(dataSource);
        erasure = new JdbcItemErasure(dataSource);
        renames = new SecurityEventsConsumer(
                new RekeyUserItems(new JdbcUserItemsRekey(dataSource)), mapper);
        purges = new PurgeCommandsConsumer(new MarkUserItemsForErasure(erasure, Clock.systemUTC()),
                new RestoreUserItems(erasure), new PurgeUserItems(erasure), mapper, stated::add);
        logLines.start();
        purgeLogger().addAppender(logLines);
    }

    @AfterEach
    void untapTheLog() {
        purgeLogger().detachAppender(logLines);
    }

    private static ch.qos.logback.classic.Logger purgeLogger() {
        return (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PurgeCommandsConsumer.class);
    }

    @Test
    @DisplayName("after a confirmed rename the saved list is still hers — under the new address")
    void the_saved_list_survives_the_rename() {
        store.add(OLD, "favourites", MEME);
        store.add(OLD, "watchlist", COMMENT);

        int moved = renames.handle(emailChanged(OLD, NEW));

        List<ItemRef> favourites = store.list(NEW, "favourites");
        assertFalse(favourites.isEmpty(),
                "THE symptom: a signed-in member whose address moved read back an empty list with"
                        + " no error at all, while their rows sat under the address they had left");
        assertEquals(List.of(MEME), favourites);
        assertEquals(2, moved, "both rows move, in one statement");
        assertEquals(List.of(COMMENT), store.list(NEW, "watchlist"), "every collection, not one");
        assertEquals(List.of(), store.list(OLD, "favourites"),
                "and nothing is left behind for the next registrant of the freed address");
    }

    @Test
    @DisplayName("a redelivered rename changes nothing — idempotent by arithmetic, not bookkeeping")
    void a_redelivered_rename_changes_nothing() {
        store.add(OLD, "favourites", MEME);
        renames.handle(emailChanged(OLD, NEW));

        assertEquals(0, renames.handle(emailChanged(OLD, NEW)),
                "the second delivery finds no row under the old address, which is the whole dedup"
                        + " story: there is no table of facts already seen");

        assertEquals(List.of(MEME), store.list(NEW, "favourites"), "still exactly one row, hers");
        assertEquals(List.of(), store.list(OLD, "favourites"));
    }

    @Test
    @DisplayName("a reference a running saga has reserved moves with her, so the closure still finds it")
    void a_reserved_reference_moves_too() {
        store.add(OLD, "favourites", MEME);
        purges.handle(mark(OLD));
        assertEquals(1, erasure.pendingOf(OLD).size(),
                "the mark reserved her reference under the address the saga named");

        assertEquals(1, renames.handle(emailChanged(OLD, NEW)),
                "the reserved row is the one row she has, and it moves");

        assertEquals(List.of(), erasure.pendingOf(OLD));
        assertEquals(1, erasure.pendingOf(NEW).size(),
                "the re-key names the TABLE, not the active view — a reserved row that stayed"
                        + " behind would be hidden from her lists AND invisible to the closure,"
                        + " which deletes the leaver's PENDING rows by address");
        purges.handle(erase(NEW));
        assertEquals(List.of(), erasure.pendingOf(NEW), "and the closure can finish the job");
    }

    @Test
    @DisplayName("a purge that reserved nothing says so on the wire, in the log and on the counter")
    void a_purge_that_reserved_nothing_says_so() throws Exception {
        // the race the fix narrows but cannot close: the deletion command arrives before the
        // rename, so it names an address this service holds nothing under
        store.add(OLD, "favourites", MEME);

        JsonNode confirmation = mapper.readTree(purges.handle(mark(NEW)).orElseThrow());

        assertEquals(0, confirmation.path("reserved").asInt(-1),
                "the answer must not read as an erasure: it used to be spelled exactly like one"
                        + " that had taken forty references out of somebody's lists");
        assertEquals("USER_CONTENT_PURGED", confirmation.path("type").asText(),
                "still a confirmation, because refusing would fail the deletion of every member"
                        + " who simply never saved anything");
        assertEquals(List.of(new Observation.PurgeReservedNothing()), stated,
                "and the zero is countable, which is the only way this is ever noticed");
        assertTrue(logLines.list.stream().anyMatch(line ->
                        line.getFormattedMessage().contains("reserved NOTHING")),
                "with a WARN naming the saga");
        assertFalse(logLines.list.stream().anyMatch(line ->
                        line.getFormattedMessage().contains(NEW)
                                || line.getFormattedMessage().contains(OLD)),
                "and still no address in any of it");
    }

    @Test
    @DisplayName("once the rename has landed the same deletion reserves — and says how much")
    void the_same_deletion_after_the_rename_reserves_her_references() throws Exception {
        store.add(OLD, "favourites", MEME);
        store.add(OLD, "watchlist", COMMENT);
        renames.handle(emailChanged(OLD, NEW));

        JsonNode confirmation = mapper.readTree(purges.handle(mark(NEW)).orElseThrow());

        assertEquals(2, confirmation.path("reserved").asInt(-1),
                "the count is what tells the two cases apart at all");
        assertEquals(List.of(), stated, "nothing to state: this deletion really did reserve");
    }

    @Test
    @DisplayName("a re-key that would collide fails loudly instead of quietly merging two people")
    void a_colliding_rekey_fails_rather_than_merges() {
        // The address takes part in one UNIQUE constraint, uq_collection_item, so this is the one
        // way the move can fail. It cannot happen while security refuses a move onto a registered
        // address — but the reading a few lines away in JdbcCollectionStore#add, where 23505 means
        // "already saved", would turn it into a reference silently left behind under the old
        // address and a member half-moved.
        store.add(OLD, "favourites", MEME);
        store.add(NEW, "favourites", MEME);
        store.add(OLD, "watchlist", COMMENT);

        assertThrows(IllegalStateException.class, () -> renames.handle(emailChanged(OLD, NEW)),
                "a duplicate must reach the loop, which does not commit the offset and tries the"
                        + " whole rename again — never a swallowed 23505");

        assertEquals(List.of(COMMENT), store.list(OLD, "watchlist"),
                "and nothing moved: one statement, so the collision takes the whole move with it"
                        + " rather than leaving the member split across two addresses");
    }

    private static String emailChanged(String oldEmail, String newEmail) {
        // the producer's shape, field for field — see EmailChangedContractTest for the pact that
        // pins it against microservice-security's real announcer
        return "{\"id\":\"" + UUID.nameUUIDFromBytes(
                (oldEmail + "|" + newEmail + "|EMAIL_CHANGED").getBytes(
                        java.nio.charset.StandardCharsets.UTF_8))
                + "\",\"type\":\"EMAIL_CHANGED\",\"oldEmail\":\"" + oldEmail
                + "\",\"email\":\"" + newEmail + "\",\"version\":1}";
    }

    private static String mark(String email) {
        return "{\"type\":\"PURGE_USER_CONTENT\",\"sagaId\":\"s-1\",\"email\":\"" + email + "\"}";
    }

    private static String erase(String email) {
        return "{\"type\":\"ERASE_USER_CONTENT\",\"sagaId\":\"s-1\",\"email\":\"" + email + "\"}";
    }
}
