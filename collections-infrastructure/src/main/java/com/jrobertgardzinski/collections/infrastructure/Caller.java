package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.identity.UserId;

import java.util.Optional;

/**
 * Who is making a request, as security's token says: the address, and the id when the token's
 * subject is one (older tokens carry the address as subject and no id).
 */
public record Caller(String email, Optional<UserId> userId) {

    static Optional<UserId> userIdFrom(String subject) {
        try {
            return Optional.of(UserId.of(subject));
        } catch (IllegalArgumentException notAnId) {
            return Optional.empty();
        }
    }
}
