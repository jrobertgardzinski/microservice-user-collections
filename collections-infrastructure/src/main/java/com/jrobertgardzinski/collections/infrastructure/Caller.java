package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.identity.UserId;

import java.util.Optional;

/** Who is making a request, as security's token says: the identity, and the address beside it. */
public record Caller(String email, UserId userId) {

    /** Empty when the subject is not an id: a token from before the cutover names nobody here. */
    static Optional<UserId> userIdFrom(String subject) {
        try {
            return Optional.of(UserId.of(subject));
        } catch (IllegalArgumentException notAnId) {
            return Optional.empty();
        }
    }
}
