package com.jrobertgardzinski.collections.infrastructure;

import au.com.dius.pact.provider.PactVerifyProvider;
import au.com.dius.pact.provider.junit5.MessageTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The saga contract's other direction, provider side: microservice-offboarding's committed pact
 * states which USER_CONTENT_PURGED fields its orchestrator reads; this test proves the REAL
 * handler — pure and broker-free, its confirmation is simply its return value — emits that shape.
 * Skipped, not failed, when the consumer repo is not checked out next to this one.
 */
@Provider("microservice-user-collections")
@PactFolder("../microservice-offboarding/pacts")
@EnabledIf(value = "consumerPactsCheckedOut",
        disabledReason = "microservice-offboarding is not checked out next to this repo")
class PurgeConfirmationPactProviderTest {

    static boolean consumerPactsCheckedOut() {
        return Files.isDirectory(Path.of("../microservice-offboarding/pacts"));
    }

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget(List.of("com.jrobertgardzinski")));
    }

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void theConfirmationShapeTheOrchestratorReliesOn(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @PactVerifyProvider("a user content purged confirmation")
    public String aUserContentPurgedConfirmation() {
        PurgeCommandsConsumer consumer = new PurgeCommandsConsumer(
                new PurgeUserItems(null) {
                    @Override
                    public int execute(String user) {
                        return 1;
                    }
                }, new ObjectMapper());
        return consumer.handle("{\"type\":\"PURGE_USER_CONTENT\","
                + "\"sagaId\":\"7d9f9e2a-1f0a-4f6e-9a1b-2c3d4e5f6a7b\","
                + "\"email\":\"leaver@example.com\"}").orElseThrow();
    }
}
