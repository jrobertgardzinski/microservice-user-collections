package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
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

        // the account-deletion saga's third participant: consume purge commands off Kafka when a
        // broker is configured (on a daemon virtual thread); without one, this simply never runs
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "").trim();
        if (!bootstrap.isEmpty()) {
            PurgeCommandsConsumer consumer =
                    new PurgeCommandsConsumer(new PurgeUserItems(store), new ObjectMapper());
            Thread.ofVirtual().name("purge-consumer").start(() -> consumer.run(bootstrap));
        }

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        .addFilter(new CorrelationFilter())
                        .get("/health", (req, res) -> res.send("OK"))
                        .get("/metrics", MetricsEndpoint::handle)
                        .register("/collections", collections))
                .build()
                .start();

        System.out.println("user-collections listening on port " + server.port());
    }
}
