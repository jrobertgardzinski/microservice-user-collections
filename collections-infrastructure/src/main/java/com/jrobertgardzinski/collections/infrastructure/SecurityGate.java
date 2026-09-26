package com.jrobertgardzinski.collections.infrastructure;

import java.util.Optional;

/**
 * Boundary gate to microservice-security: resolves a bearer token to the signed-in {@link Caller},
 * or empty when the token is missing, invalid or expired. Every collections operation is a write to
 * the caller's own data, so all of them require a resolved user.
 */
public interface SecurityGate {

    Optional<Caller> callerFor(String accessToken);
}
