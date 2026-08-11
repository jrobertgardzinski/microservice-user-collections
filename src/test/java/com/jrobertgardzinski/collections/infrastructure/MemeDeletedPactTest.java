package com.jrobertgardzinski.collections.infrastructure;

import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
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
 * The CASCADE's first hop as a consumer-driven contract: this service states, in a file
 * microservice-memes verifies against its real producer, the exact MEME_DELETED shape it acts on —
 * and proves the statement by driving the REAL {@link CascadeConsumer} with the pact's own payload
 * instead of a hand-written string.
 *
 * <p>Only the fields this consumer reads are in the contract ({@code type} and {@code memeId});
 * memes also puts an {@code eventId} on the wire, and a tolerant reader must not pin what it does
 * not use — pinning it would make an unrelated producer change look like a broken contract.
 *
 * <h2>Why the TOPIC is in the pact's metadata</h2>
 *
 * The audit of 26.07 found the deletion cascade's worst structural blind spot: nothing asserted the
 * topic NAME. A typo on either side deletes nothing, reports nothing, and leaves CI entirely green —
 * the events simply fall on a topic nobody reads. A message pact's metadata is the natural home for
 * that name (it is transport, not payload), and putting it here makes the topic part of the
 * VERIFIED contract rather than a coincidence: {@code MemeDeletedPactProviderTest} in
 * microservice-memes answers this interaction with a real {@link org.apache.kafka.clients.producer.ProducerRecord}
 * and reports {@code record.topic()} as the metadata, so a producer that starts publishing
 * somewhere else fails there, against THIS file.
 *
 * @see CascadeTopicNamesTest for the same name pinned against the subscription this service opens
 */
@Epic("Contract")
@Feature("Cascade events")
@Story("Meme deleted")
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-memes", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class MemeDeletedPactTest {

    private static final String MEME = "3a8f0f6e-1b2c-4d5e-8f90-1a2b3c4d5e6f";

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final CascadeConsumer cascade =
            new CascadeConsumer(new PurgeDeletedItem(store), new ObjectMapper());

    @Pact(consumer = "microservice-user-collections")
    MessagePact memeDeleted(MessagePactBuilder builder) {
        return builder.expectsToReceive("a meme deleted announcement")
                // the transport half of the contract — see the class comment
                .withMetadata(Map.of("topic", CascadeConsumer.MEMES_TOPIC))
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "MEME_DELETED")
                        // a uuid, not merely a string: the cascade REJECTS anything else at the
                        // boundary (CascadeConsumer#isNotAnId), so a laxer id would be dropped
                        // rather than acted on — that is a contract term, not an implementation
                        // detail
                        .uuid("memeId", MEME))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "memeDeleted")
    @DisplayName("the announcement memes publishes takes every saved reference to the dead meme")
    void every_reference_to_the_deleted_meme_goes(List<Message> messages) {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("bob@example.com", "watchlist", new ItemRef("meme", MEME));

        int removed = cascade.handle(CascadeConsumer.MEMES_TOPIC,
                messages.get(0).contentsAsString());

        assertEquals(2, removed, "the real consumer, driven by the pact's own payload");
        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertTrue(store.list("bob@example.com", "watchlist").isEmpty());
    }

    @Test
    @PactTestFor(pactMethod = "memeDeleted")
    @DisplayName("the topic in the pact is the topic this service actually subscribes to")
    void the_pact_names_the_topic_this_consumer_listens_on(List<Message> messages) {
        // the pact is only worth as much as its transport claim: if the metadata ever drifts from
        // the subscription, the provider would be verified against a topic nobody here reads
        assertEquals(CascadeConsumer.MEMES_TOPIC,
                messages.get(0).getMetadata().get("topic"),
                "the recorded topic must be the one CascadeConsumer#run subscribes to");
    }
}
