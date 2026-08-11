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
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The consumer's half of the account-deletion saga contract: the pact states the exact shape of
 * every {@code content-commands} event this service acts on, and proves each one by driving the
 * real consumer with the pact's payload. The generated pact (pacts/, committed) is verified against
 * the REAL orchestrator by microservice-offboarding's provider tests. Only the fields this consumer
 * reads are in the contract; the producer may add more (tolerant reader).
 *
 * <p>THREE commands now, because the saga has two phases (ADR 0007) and this service is finally
 * part of both. Note what is NOT here and is in the memes and comments pacts: a policy. The refs
 * are opaque, so there is no per-item fate to choose — the closure carries a policy this consumer
 * simply never reads, which is exactly what a tolerant reader is allowed to do.
 */
@Epic("Contract")
@Feature("Account-deletion saga")
@Story("Commands consumed")
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-offboarding", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class PurgeCommandsContractTest {

    private static final String LEAVER = "leaver@example.com";

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final PurgeCommandsConsumer consumer = new PurgeCommandsConsumer(
            new MarkUserItemsForErasure(store, Clock.systemUTC()), new RestoreUserItems(store),
            new PurgeUserItems(store), new ObjectMapper());

    @Pact(consumer = "microservice-user-collections")
    MessagePact purgeCommand(MessagePactBuilder builder) {
        return builder.expectsToReceive("a purge user content command")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "PURGE_USER_CONTENT")
                        .uuid("sagaId")
                        .stringType("email", LEAVER))
                .toPact();
    }

    @Pact(consumer = "microservice-user-collections")
    MessagePact eraseCommand(MessagePactBuilder builder) {
        return builder.expectsToReceive("an erase user content command closing the saga")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "ERASE_USER_CONTENT")
                        .uuid("sagaId")
                        .stringType("email", LEAVER))
                .toPact();
    }

    @Pact(consumer = "microservice-user-collections")
    MessagePact restoreCommand(MessagePactBuilder builder) {
        return builder.expectsToReceive("a restore user content command compensating the saga")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "RESTORE_USER_CONTENT")
                        .uuid("sagaId")
                        .stringType("email", LEAVER))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "purgeCommand")
    void marksTheLeaversCollectionsAndConfirms(List<Message> messages) {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));

        Optional<String> confirmation = consumer.handle(messages.get(0).contentsAsString());

        assertTrue(confirmation.isPresent(), "the saga participant must confirm its mark");
        assertEquals(List.of(), store.list(LEAVER, "favourites"),
                "the leaver's list is empty to everyone who reads it");
        assertEquals(1, store.pendingOf(LEAVER).size(),
                "and the row is still there — reserved, which is what makes this compensatable");
    }

    @Test
    @PactTestFor(pactMethod = "eraseCommand")
    void erasesWhatItReservedAndDoesNotConfirm(List<Message> messages) {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));
        new MarkUserItemsForErasure(store, Clock.systemUTC()).execute(LEAVER);

        Optional<String> confirmation = consumer.handle(messages.get(0).contentsAsString());

        assertFalse(confirmation.isPresent(),
                "the closure ENDS the case — answering it would tell the orchestrator something"
                        + " it has already decided");
        assertEquals(List.of(), store.pendingOf(LEAVER), "the reservation is gone with the rows");
    }

    @Test
    @PactTestFor(pactMethod = "restoreCommand")
    void restoresWhatItReservedAndDoesNotConfirm(List<Message> messages) {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));
        new MarkUserItemsForErasure(store, Clock.systemUTC()).execute(LEAVER);

        Optional<String> confirmation = consumer.handle(messages.get(0).contentsAsString());

        assertFalse(confirmation.isPresent(), "the compensation ends the case too");
        assertEquals(List.of(new ItemRef("meme", "42")), store.list(LEAVER, "favourites"),
                "the leaver's list is whole again — the point of the whole two-phase design");
    }
}
