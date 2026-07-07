package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import io.helidon.webserver.WebServer;

import javax.sql.DataSource;

/**
 * Boots the Helidon 4 SE WebServer (virtual threads) and wires the use cases to their adapters.
 * The sixth flavour in the portfolio: imperative and blocking, yet scaling on Loom. Port comes
 * from {@code COLLECTIONS_PORT} (default 8092 — next free after the idp's 8091).
 *
 * <p>Storage is Postgres when {@code DB_URL} is set, else in-memory H2. Every collections route is
 * gated by microservice-security's JWKS ({@code SECURITY_URL}, default the local security).
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("COLLECTIONS_PORT", "8092"));
        String securityUrl = System.getenv().getOrDefault("SECURITY_URL", "http://localhost:8080");

        DataSource dataSource = Database.migratedDataSource();
        CollectionStore store = new JdbcCollectionStore(dataSource);
        SecurityGate gate = new JwtSecurityGate(securityUrl);

        CollectionsApi collections = new CollectionsApi(
                new SaveItem(store), new RemoveItem(store), new ListItems(store), gate);

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        .get("/health", (req, res) -> res.send("OK"))
                        .register("/collections", collections))
                .build()
                .start();

        System.out.println("user-collections listening on port " + server.port());
    }
}
