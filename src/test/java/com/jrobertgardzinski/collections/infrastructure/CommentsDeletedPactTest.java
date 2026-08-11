package com.jrobertgardzinski.collections.infrastructure;

import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslJsonRootValue;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.messaging.Message;
import au.com.dius.pact.core.model.messaging.MessagePact;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CASCADE's second hop as a consumer-driven contract: microservice-comments dropped the dead
 * meme's thread and is the only service that knows which comments went, so this file states the
 * exact COMMENTS_DELETED shape this service needs in order to drop the references to them — and
 * proves it by driving the REAL {@link CascadeConsumer} with the pact's own payload.
 *
 * <p>Three fields are contract, and each for a different reason:
 * <ul>
 *   <li>{@code type} — both cascade topics are shared, so the type is the only filter;</li>
 *   <li>{@code commentIds} — the whole point of the event; nothing here can derive them;</li>
 *   <li>{@code memeId} — NOT used to purge anything, and still required: it is the cascade's only
 *       handle in the log and the event's partition key, and this service DROPS an event without
 *       one (CascadeConsumer#onCommentsDeleted). A consumer that rejects an event must say so in
 *       its pact, or the producer has no way of knowing.</li>
 * </ul>
 *
 * <p>{@code id} and {@code version} ride the envelope and are deliberately absent: this consumer
 * reads neither, and a tolerant reader pins only what it uses.
 *
 * <p>The {@code topic} metadata is the audit's finding made testable — see
 * {@link MemeDeletedPactTest} for the full argument. {@code CommentsDeletedPactProviderTest} in
 * microservice-comments answers this interaction with a real
 * {@link org.apache.kafka.clients.producer.ProducerRecord} and reports its {@code topic()}, so a
 * producer that moves the announcement elsewhere fails there, against THIS file.
 */
@Epic("Contract")
@Feature("Cascade events")
@Story("Comments deleted")
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-comments", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class CommentsDeletedPactTest {

    private static final String MEME = "3a8f0f6e-1b2c-4d5e-8f90-1a2b3c4d5e6f";
    private static final String COMMENT = "11111111-1111-4111-8111-111111111111";

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final CascadeConsumer cascade =
            new CascadeConsumer(new PurgeDeletedItem(store), new ObjectMapper());

    @Pact(consumer = "microservice-user-collections")
    MessagePact commentsDeleted(MessagePactBuilder builder) {
        return builder.expectsToReceive("a comments deleted announcement")
                .withMetadata(Map.of("topic", CascadeConsumer.COMMENTS_TOPIC))
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "COMMENTS_DELETED")
                        .uuid("memeId", MEME)
                        // at least one id — an empty announcement states no fact, and comments
                        // does not send one (MemesEventsListener returns early on an empty thread).
                        // uuids for the same reason as memeId: anything else is dropped here
                        .minArrayLike("commentIds", 1, PactDslJsonRootValue.uuid(COMMENT), 1))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "commentsDeleted")
    @DisplayName("the announcement comments publishes takes the saved references it names")
    void the_named_comment_references_go(List<Message> messages) {
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT));
        store.add("bob@example.com", "favourites", new ItemRef("comment", COMMENT));
        // the id collision that the item type is the only defence against: same id, other kind
        store.add("bob@example.com", "favourites", new ItemRef("meme", COMMENT));

        int removed = cascade.handle(CascadeConsumer.COMMENTS_TOPIC,
                messages.get(0).contentsAsString());

        assertEquals(2, removed, "the real consumer, driven by the pact's own payload");
        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertEquals(List.of(new ItemRef("meme", COMMENT)),
                store.list("bob@example.com", "favourites"),
                "a comment deletion must never take the meme that happens to share its id");
    }

    @Test
    @PactTestFor(pactMethod = "commentsDeleted")
    @DisplayName("the topic in the pact is the topic this service actually subscribes to")
    void the_pact_names_the_topic_this_consumer_listens_on(List<Message> messages) {
        assertEquals(CascadeConsumer.COMMENTS_TOPIC,
                messages.get(0).getMetadata().get("topic"),
                "the recorded topic must be the one CascadeConsumer#run subscribes to");
    }
}
