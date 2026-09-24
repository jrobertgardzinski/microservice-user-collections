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
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import com.jrobertgardzinski.collections.application.UserItemsRekey;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The consumer's half of the contract for the one fact this service reads off
 * {@code security-events}: an address change. The pact states the shape, the test proves it by
 * driving the REAL {@link SecurityEventsConsumer} with the pact's own payload, and the generated
 * file (pacts/, committed) is what microservice-security verifies against its REAL producer —
 * {@code EmailChangedAnnouncer} — so a renamed or dropped field goes red in the producer's build
 * rather than in a live stack (ADR 0003).
 *
 * <p>Three fields are pinned and no more (tolerant reader): the {@code type} that selects the fact,
 * {@code oldEmail} — the address every row here is keyed by today — and {@code email}, which is the
 * NEW one. That last pair is the whole reason this pact is worth having: the producer calls the
 * subject's current address {@code email} on every fact on that topic, so the intuitive reading of
 * "email" here is precisely the wrong one, and a consumer that swapped the two would move every
 * member onto the address they had just left. The {@code id} and {@code version} the producer also
 * sends are deliberately absent — nothing here reads them, and a pact that pins what it does not
 * use is a pact that fails for reasons nobody has to care about.
 *
 * <p>No topic in the metadata, unlike {@link MemeDeletedPactTest} — and that is the estate's
 * agreement rather than an oversight: security answers all three content services' EMAIL_CHANGED
 * pacts from one payload-only provider method, so metadata here would be verified against nothing.
 * The name is pinned on this side instead, by {@link SecurityEventsConsumerLoopTest}.
 */
@Epic("Contract")
@Feature("Address changes")
@Story("Email changed")
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-security", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class EmailChangedContractTest {

    /** What the real use case was asked to move, captured instead of performed. */
    private final List<String> moves = new ArrayList<>();
    private final UserItemsRekey rekey = (oldEmail, newEmail) -> {
        moves.add(oldEmail + " -> " + newEmail);
        return 0;
    };
    private final SecurityEventsConsumer consumer =
            new SecurityEventsConsumer(new RekeyUserItems(rekey), new ObjectMapper());

    @Pact(consumer = "microservice-user-collections")
    MessagePact emailChanged(MessagePactBuilder builder) {
        return builder.expectsToReceive("an email changed fact")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "EMAIL_CHANGED")
                        .stringType("oldEmail", "alice@old.example.com")
                        .stringType("email", "alice@new.example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "emailChanged")
    @DisplayName("the rows move FROM oldEmail TO email — the fact's 'email' is the new address")
    void rekeysFromTheOldAddressToTheNewOne(List<Message> messages) {
        consumer.handle(messages.get(0).contentsAsString());

        assertEquals(List.of("alice@old.example.com -> alice@new.example.com"), moves,
                "the real consumer, driven by the pact's own payload");
    }
}
