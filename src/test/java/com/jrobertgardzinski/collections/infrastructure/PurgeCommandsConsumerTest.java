package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The saga participant's core: a purge command clears the user and yields the right confirmation. */
class PurgeCommandsConsumerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final PurgeCommandsConsumer consumer =
            new PurgeCommandsConsumer(new PurgeUserItems(store), mapper);

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
}
