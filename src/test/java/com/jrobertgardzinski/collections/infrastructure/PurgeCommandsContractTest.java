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
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The consumer's half of the account-deletion saga contract: the pact states the exact shape of
 * the {@code content-commands} event this service acts on, and proves it by driving the real
 * consumer with the pact's payload. Unlike memes/comments there is no policy axis — refs are
 * opaque, the purge is wholesale. The generated pact (pacts/, committed) is verified against the
 * REAL orchestrator by microservice-security's provider tests. Only the fields this consumer reads
 * are in the contract; the producer may add more (tolerant reader).
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-security", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class PurgeCommandsContractTest {

    private final List<String> purged = new ArrayList<>();
    private final PurgeCommandsConsumer consumer = new PurgeCommandsConsumer(
            new PurgeUserItems(null) {
                @Override
                public int execute(String user) {
                    purged.add(user);
                    return 1;
                }
            }, new ObjectMapper());

    @Pact(consumer = "microservice-user-collections")
    MessagePact purgeCommand(MessagePactBuilder builder) {
        return builder.expectsToReceive("a purge user content command")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "PURGE_USER_CONTENT")
                        .uuid("sagaId")
                        .stringType("email", "leaver@example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "purgeCommand")
    void purgesTheLeaversCollectionsAndConfirms(List<Message> messages) {
        Optional<String> confirmation = consumer.handle(messages.get(0).contentsAsString());
        assertEquals(List.of("leaver@example.com"), purged);
        assertTrue(confirmation.isPresent(), "the saga participant must confirm its purge");
    }
}
