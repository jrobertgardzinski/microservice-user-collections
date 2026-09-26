package com.jrobertgardzinski.collections.httpsteps;

import com.jrobertgardzinski.collections.infrastructure.Caller;
import com.jrobertgardzinski.collections.infrastructure.SecurityGate;
import com.jrobertgardzinski.identity.UserId;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * A test double for the JWT gate: the bearer token IS the user (so "Bearer alice" resolves to
 * alice, with an id derived from the name), a blank token resolves to nobody. Lets the HTTP scenarios exercise the boundary without
 * a running microservice-security or real tokens.
 */
class FakeGate implements SecurityGate {

    @Override
    public Optional<Caller> callerFor(String accessToken) {
        if (accessToken == null || accessToken.isBlank()) {
            return Optional.empty();
        }
        UUID id = UUID.nameUUIDFromBytes(accessToken.getBytes(StandardCharsets.UTF_8));
        return Optional.of(new Caller(accessToken, Optional.of(new UserId(id))));
    }
}
