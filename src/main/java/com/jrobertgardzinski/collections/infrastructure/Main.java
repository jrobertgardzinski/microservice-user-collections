package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.webserver.WebServer;

/**
 * Boots the Helidon 4 SE WebServer (virtual threads) and wires the use cases to their adapters.
 * The sixth flavour in the portfolio: imperative and blocking, yet scaling on Loom. Port comes
 * from {@code COLLECTIONS_PORT} (default 8092 — next free after the idp's 8091).
 *
 * <p>This slice is the walking skeleton: it stands up and answers {@code /health}. The routes,
 * persistence, auth and the account-deletion consumer land in the following slices.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("COLLECTIONS_PORT", "8092"));

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        .get("/health", (req, res) -> res.send("OK")))
                .build()
                .start();

        System.out.println("user-collections listening on port " + server.port());
    }
}
