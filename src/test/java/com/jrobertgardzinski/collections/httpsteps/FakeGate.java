package com.jrobertgardzinski.collections.httpsteps;

import com.jrobertgardzinski.collections.infrastructure.SecurityGate;

import java.util.Optional;

/**
 * A test double for the JWT gate: the bearer token IS the user (so "Bearer alice" resolves to
 * alice), a blank token resolves to nobody. Lets the HTTP scenarios exercise the boundary without
 * a running microservice-security or real tokens.
 */
class FakeGate implements SecurityGate {

    @Override
    public Optional<String> userFor(String accessToken) {
        return accessToken == null || accessToken.isBlank() ? Optional.empty() : Optional.of(accessToken);
    }
}
