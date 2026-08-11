package com.jrobertgardzinski.collections.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cascade's event contract, driven through the broker-free {@code handle(topic, payload)}
 * seam. The claims, in the order they matter:
 *
 * <ol>
 *   <li>a deletion takes every user's reference to the deleted thing;</li>
 *   <li>it takes NOTHING else — not the same id under another type, not a comment that was not
 *       named, not another meme;</li>
 *   <li>a foreign event type on either shared topic is silence, not noise;</li>
 *   <li>a poison pill is dropped with a WARN that never carries the payload;</li>
 *   <li>the log lines carry the memeId (an id, not PII) and the number of refs removed.</li>
 * </ol>
 */
@Epic("Infrastructure")
@Feature("Deletion cascade")
@Story("Event handling")
class CascadeConsumerTest {

    private static final String MEME = "3a8f0f6e-1b2c-4d5e-8f90-1a2b3c4d5e6f";
    private static final String COMMENT_1 = "11111111-1111-4111-8111-111111111111";
    private static final String COMMENT_2 = "22222222-2222-4222-8222-222222222222";
    private static final String COMMENT_3 = "33333333-3333-4333-8333-333333333333";

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final CascadeConsumer cascade =
            new CascadeConsumer(new PurgeDeletedItem(store), mapper);

    private final ListAppender<ILoggingEvent> logLines = new ListAppender<>();

    @BeforeEach
    void tapTheLog() {
        logLines.start();
        cascadeLogger().addAppender(logLines);
    }

    @AfterEach
    void untapTheLog() {
        cascadeLogger().detachAppender(logLines);
    }

    private static Logger cascadeLogger() {
        return (Logger) LoggerFactory.getLogger(CascadeConsumer.class);
    }

    // ---- MEME_DELETED, on memes-events ----

    @Test
    void a_deleted_meme_loses_every_users_reference_to_it() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("bob@example.com", "watchlist", new ItemRef("meme", MEME));

        assertEquals(2, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted(MEME)));

        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertTrue(store.list("bob@example.com", "watchlist").isEmpty());
    }

    @Test
    void a_deleted_meme_does_not_take_the_comment_that_shares_its_id() {
        // the two sources mint ids independently, so a collision is possible and the item_type is
        // the only thing standing between a meme's deletion and somebody's saved comment
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("comment", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("meme", COMMENT_1));

        assertEquals(1, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted(MEME)));

        assertEquals(2, store.list("alice@example.com", "favourites").size(),
                "only the (meme, " + MEME + ") ref may go");
        assertTrue(store.list("alice@example.com", "favourites")
                .contains(new ItemRef("comment", MEME)));
    }

    @Test
    void the_same_meme_deletion_twice_is_free() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));

        assertEquals(1, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted(MEME)));
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted(MEME)),
                "at-least-once redelivery must not be an error");
    }

    // ---- COMMENTS_DELETED, on comments-events ----

    @Test
    void deleted_comments_lose_exactly_the_refs_the_event_names() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));
        store.add("bob@example.com", "favourites", new ItemRef("comment", COMMENT_1));
        store.add("bob@example.com", "favourites", new ItemRef("comment", COMMENT_2));
        store.add("bob@example.com", "favourites", new ItemRef("comment", COMMENT_3));
        store.add("bob@example.com", "favourites", new ItemRef("meme", COMMENT_1));

        assertEquals(3, cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                commentsDeleted(MEME, COMMENT_1, COMMENT_2)));

        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertEquals(List.of(new ItemRef("meme", COMMENT_1), new ItemRef("comment", COMMENT_3)),
                store.list("bob@example.com", "favourites"),
                "the unnamed comment and the meme that shares an id both stay");
    }

    @Test
    void a_comments_deleted_with_an_empty_list_removes_nothing_and_raises_nothing() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        assertEquals(0, cascade.handle(CascadeConsumer.COMMENTS_TOPIC, commentsDeleted(MEME)));
        assertEquals(1, store.list("alice@example.com", "favourites").size());
    }

    @Test
    void unusable_ids_inside_a_comments_deleted_are_skipped_and_the_rest_still_go() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        assertEquals(1, cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                "{\"id\":\"e-1\",\"type\":\"COMMENTS_DELETED\",\"memeId\":\"" + MEME
                        + "\",\"commentIds\":[\"" + COMMENT_1 + "\",\"\",\"not-an-id\"],"
                        + "\"version\":1}"));

        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertTrue(warned("are not ids and were skipped"),
                "the operator must learn the producer sent garbage");
        assertFalse(logged("not-an-id"),
                "but the garbage itself is untrusted content and stays out of the log");
    }

    // ---- the topics are shared: everything else is silence ----

    @Test
    void a_foreign_event_type_on_either_topic_is_ignored_without_a_word() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC,
                "{\"type\":\"MEME_UPLOADED\",\"memeId\":\"" + MEME + "\"}"));
        assertEquals(0, cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"s-1\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}"));
        assertEquals(0, cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                "{\"type\":\"COMMENT_POSTED\",\"memeId\":\"" + MEME + "\"}"));

        assertEquals(2, store.list("alice@example.com", "favourites").size(), "nothing purged");
        assertTrue(logLines.list.isEmpty(),
                "these topics carry other conversations all day — a line per foreign event would"
                        + " bury the ones that matter. Logged: " + messages());
    }

    @Test
    void the_saga_confirmation_sharing_the_comments_topic_never_leaks_its_email() {
        // comments-events carries this service's OWN saga confirmations too; the cascade must
        // neither act on them nor echo the leaver's address while ignoring them
        cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"s-1\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");

        assertFalse(logged("leaver@example.com"), "PII must not reach a log line");
    }

    @Test
    void an_event_of_the_right_type_on_the_wrong_topic_is_not_ours() {
        // the topic is half the contract: COMMENTS_DELETED means comments only on comments-events
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC,
                commentsDeleted(MEME, COMMENT_1)));
        assertEquals(0, cascade.handle(CascadeConsumer.COMMENTS_TOPIC, memeDeleted(MEME)));
        assertEquals(1, store.list("alice@example.com", "favourites").size());
    }

    // ---- poison pills ----

    @Test
    void a_malformed_payload_is_dropped_and_never_echoed() {
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC,
                "not json at all, but with leaver@example.com inside"));

        assertTrue(warned("not valid JSON"), "the drop must leave a trace: " + messages());
        assertFalse(logged("leaver@example.com"),
                "the payload may carry anything — it never reaches the log");
    }

    @Test
    void a_meme_deleted_without_a_usable_id_is_dropped_with_a_warning() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));

        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC,
                "{\"type\":\"MEME_DELETED\",\"eventId\":\"e-1\"}"), "no memeId at all");
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted("")), "blank");
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted("   ")), "spaces");
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted("42")),
                "an id off-contract: the event says uuid");

        assertEquals(1, store.list("alice@example.com", "favourites").size(),
                "a purge with nothing to purge must never turn into a purge of everything");
        assertEquals(4, logLines.list.stream()
                        .filter(line -> line.getFormattedMessage().contains("MEME_DELETED whose"))
                        .count(),
                "each drop gets its own WARN: " + messages());
    }

    @Test
    void a_comments_deleted_without_a_usable_meme_id_is_dropped_with_a_warning() {
        // the memeId is the cascade's only handle in the log; an event without one cannot be
        // audited afterwards, so it is off-contract even though the comment ids look fine
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        assertEquals(0, cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                "{\"id\":\"e-1\",\"type\":\"COMMENTS_DELETED\",\"commentIds\":[\"" + COMMENT_1
                        + "\"],\"version\":1}"));

        assertEquals(1, store.list("alice@example.com", "favourites").size());
        assertTrue(warned("COMMENTS_DELETED whose memeId"), messages().toString());
    }

    @Test
    void a_null_payload_is_a_drop_not_a_crash() {
        assertEquals(0, cascade.handle(CascadeConsumer.MEMES_TOPIC, null));
        assertTrue(warned("not valid JSON"));
    }

    // ---- what the successful lines say ----

    @Test
    void a_successful_cascade_logs_the_meme_id_and_the_number_of_refs() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("bob@example.com", "favourites", new ItemRef("meme", MEME));

        cascade.handle(CascadeConsumer.MEMES_TOPIC, memeDeleted(MEME));

        assertTrue(logLines.list.stream().anyMatch(line ->
                        line.getFormattedMessage().contains(MEME)
                                // "2", not just any 2 — a uuid is full of digits
                                && line.getFormattedMessage().contains("removed 2 ")),
                "the memeId is an id, not PII — it is the handle an operator needs, together"
                        + " with how many refs went: " + messages());
    }

    @Test
    void a_comments_cascade_logs_the_meme_it_belonged_to() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));

        cascade.handle(CascadeConsumer.COMMENTS_TOPIC, commentsDeleted(MEME, COMMENT_1));

        assertTrue(logLines.list.stream().anyMatch(line ->
                        line.getFormattedMessage().contains(MEME)),
                "a comment cascade is only traceable through the meme that caused it: "
                        + messages());
    }

    // ---- the wiring the loop depends on ----

    @Test
    void the_cascade_runs_in_its_own_consumer_group() {
        // the single line that keeps the saga's offsets, lag and lifecycle out of reach of a
        // best-effort cleanup — if this ever equals the saga consumer's group, the two loops
        // start stealing each other's partitions
        assertEquals(CascadeConsumer.GROUP_ID,
                CascadeConsumer.consumerProps("localhost:9092").getProperty("group.id"));
        assertNotEquals(
                PurgeCommandsConsumer.consumerProps("localhost:9092").getProperty("group.id"),
                CascadeConsumer.GROUP_ID,
                "the cascade must never share the saga consumer's group");
        assertEquals("false",
                CascadeConsumer.consumerProps("localhost:9092").getProperty("enable.auto.commit"),
                "offsets move when the batch has been handled, not on a timer");
        assertEquals("earliest",
                CascadeConsumer.consumerProps("localhost:9092").getProperty("auto.offset.reset"),
                "the first deployment is meant to walk the retained history and clear the dead"
                        + " rows that accumulated before the cascade existed");
    }

    @Test
    void the_cascade_does_not_touch_the_saga_topics() {
        // the mirror of the group check: whatever else changes, this loop must not start
        // consuming the command topic the saga participant owns
        assertFalse(List.of(CascadeConsumer.MEMES_TOPIC, CascadeConsumer.COMMENTS_TOPIC)
                        .contains(PurgeCommandsConsumer.COMMANDS_TOPIC),
                "the saga's command topic is not a cascade topic");
        assertFalse(List.of(CascadeConsumer.MEMES_TOPIC, CascadeConsumer.COMMENTS_TOPIC)
                        .contains(PurgeCommandsConsumer.EVENTS_TOPIC),
                "and neither is the topic it confirms on");
    }

    // ---- helpers ----

    /** The MEME_DELETED shape microservice-memes really publishes (KafkaMemeEvents). */
    private static String memeDeleted(String memeId) {
        return "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + memeId + "\",\"eventId\":\"e-1\"}";
    }

    /** The agreed COMMENTS_DELETED envelope (v1). */
    private static String commentsDeleted(String memeId, String... commentIds) {
        StringBuilder ids = new StringBuilder();
        for (String commentId : commentIds) {
            ids.append(ids.isEmpty() ? "" : ",").append('"').append(commentId).append('"');
        }
        return "{\"id\":\"00000000-0000-4000-8000-000000000001\",\"type\":\"COMMENTS_DELETED\","
                + "\"memeId\":\"" + memeId + "\",\"commentIds\":[" + ids + "],\"version\":1}";
    }

    private boolean warned(String fragment) {
        return logLines.list.stream().anyMatch(line ->
                line.getFormattedMessage().contains(fragment)
                        && line.getLevel().toString().equals("WARN"));
    }

    private boolean logged(String fragment) {
        return logLines.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains(fragment));
    }

    private List<String> messages() {
        return logLines.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }
}
