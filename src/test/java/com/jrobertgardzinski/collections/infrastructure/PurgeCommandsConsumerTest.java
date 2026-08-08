package com.jrobertgardzinski.collections.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The saga participant's core: a purge command clears the user and yields the right confirmation.
 * The logging is scrutinised alongside (ListAppender, the same pattern as the comments service's
 * listener test): purge commands carry the leaver's e-mail — PII that must not reach a log line,
 * neither from a malformed payload's WARN nor from the successful purge's INFO.
 */
class PurgeCommandsConsumerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final PurgeCommandsConsumer consumer = new PurgeCommandsConsumer(
            new MarkUserItemsForErasure(store, java.time.Clock.systemUTC()),
            new RestoreUserItems(store), new PurgeUserItems(store), mapper);

    private final ListAppender<ILoggingEvent> logLines = new ListAppender<>();

    @BeforeEach
    void tapTheLog() {
        logLines.start();
        consumerLogger().addAppender(logLines);
    }

    @AfterEach
    void untapTheLog() {
        consumerLogger().detachAppender(logLines);
    }

    private static Logger consumerLogger() {
        return (Logger) LoggerFactory.getLogger(PurgeCommandsConsumer.class);
    }

    @Test
    void a_purge_command_clears_the_user_and_confirms() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        store.add("alice@example.com", "watchlist", new ItemRef("comment", "7"));

        Optional<String> confirmation = consumer.handle(
                "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"alice@example.com\",\"sagaId\":\"s-1\"}");

        assertTrue(store.list("alice@example.com", "favourites").isEmpty(), "collections purged");
        assertTrue(store.list("alice@example.com", "watchlist").isEmpty());

        JsonNode event = mapper.readTree(confirmation.orElseThrow());
        assertEquals("USER_CONTENT_PURGED", event.path("type").asText());
        assertEquals("s-1", event.path("sagaId").asText());
        assertEquals("alice@example.com", event.path("email").asText());
    }

    @Test
    void a_command_of_another_type_is_ignored() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));

        assertTrue(consumer.handle("{\"type\":\"SOMETHING_ELSE\",\"email\":\"alice@example.com\"}").isEmpty());
        assertEquals(1, store.list("alice@example.com", "favourites").size(), "nothing purged");
    }

    @Test
    void a_malformed_command_is_dropped_not_thrown() {
        assertTrue(consumer.handle("not json at all").isEmpty());
    }

    @Test
    void a_malformed_payload_is_not_echoed_into_the_log() {
        // even a broken purge command may carry the leaver's e-mail — the WARN reports the size
        // and "not valid JSON", never the bytes themselves
        assertTrue(consumer.handle("not json at all, but with leaver@example.com inside").isEmpty());

        assertTrue(logLines.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("malformed")
                                && event.getFormattedMessage().contains("not valid JSON")),
                "the drop must still leave a trace in the log");
        assertFalse(logLines.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("leaver@example.com")),
                "the payload (with its PII) must not be echoed into the log");
    }

    @Test
    void a_successful_purge_logs_the_saga_id_never_the_email() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));

        consumer.handle(
                "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"alice@example.com\",\"sagaId\":\"s-7\"}");

        assertTrue(logLines.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("s-7")),
                "the saga id identifies the run in the log");
        assertFalse(logLines.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("alice@example.com")),
                "the leaver's e-mail is PII and stays out of every log line");
    }

    @Test
    void a_purge_command_without_an_email_is_dropped_without_a_confirmation() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));

        assertTrue(consumer.handle("{\"type\":\"PURGE_USER_CONTENT\",\"sagaId\":\"s-2\"}").isEmpty(),
                "a missing email must not produce a confirmation");
        assertTrue(consumer.handle(
                        "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"\",\"sagaId\":\"s-3\"}").isEmpty(),
                "an empty email must not produce a confirmation");
        assertEquals(1, store.list("alice@example.com", "favourites").size(), "nothing purged");
    }
}
