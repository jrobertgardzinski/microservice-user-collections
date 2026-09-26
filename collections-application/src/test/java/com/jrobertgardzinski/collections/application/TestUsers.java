package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.identity.UserId;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** The tests speak in names; the store speaks in ids. One id per name, the same on every run. */
public final class TestUsers {

    private TestUsers() {
    }

    public static UserId u(String name) {
        return new UserId(UUID.nameUUIDFromBytes(("user:" + name).getBytes(StandardCharsets.UTF_8)));
    }
}
