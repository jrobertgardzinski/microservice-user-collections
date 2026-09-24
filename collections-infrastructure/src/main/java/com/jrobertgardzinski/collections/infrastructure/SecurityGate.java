package com.jrobertgardzinski.collections.infrastructure;

import java.util.Optional;

/**
 * Boundary gate to microservice-security: resolves a bearer token to the signed-in user's e-mail,
 * or empty when the token is missing, invalid or expired. Every collections operation is a write to
 * the caller's own data, so all of them require a resolved user.
 */
public interface SecurityGate {

    Optional<String> userFor(String accessToken);
}
