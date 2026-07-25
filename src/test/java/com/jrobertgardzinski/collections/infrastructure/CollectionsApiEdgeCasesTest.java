package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The boundary's refusals, black-box over HTTP: without a resolvable token nothing answers but
 * 401, and a path segment wider than the schema's columns answers 400 at the edge — never a
 * SQLException-turned-500 from deep inside the JDBC store. The happy paths live in the Gherkin
 * scenarios; only the edges are here.
 */
class CollectionsApiEdgeCasesTest {

    private static final String VALID_TOKEN = "valid-token";

    private static WebServer server;
    private static String baseUrl;
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() {
        InMemoryCollectionStore store = new InMemoryCollectionStore();
        // only the one known token resolves — anything else is the gate saying "nobody"
        SecurityGate gate = token ->
                VALID_TOKEN.equals(token) ? Optional.of("alice@example.com") : Optional.empty();
        CollectionsApi api = new CollectionsApi(
                new SaveItem(store), new RemoveItem(store), new ListItems(store), gate);
        server = WebServer.builder()
                .port(0)
                .routing(routing -> routing.register("/collections", api))
                .build()
                .start();
        baseUrl = "http://localhost:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.stop();
    }

    @Test
    void without_authorization_every_route_answers_401() {
        assertEquals(401, statusOf("GET", "/collections/favourites/items", null));
        assertEquals(401, statusOf("PUT", "/collections/favourites/items/meme/42", null));
        assertEquals(401, statusOf("DELETE", "/collections/favourites/items/meme/42", null));
    }

    @Test
    void a_garbage_bearer_token_answers_401() {
        assertEquals(401, statusOf("GET", "/collections/favourites/items", "Bearer garbage"));
        assertEquals(401, statusOf("PUT", "/collections/favourites/items/meme/42", "Bearer garbage"));
    }

    @Test
    void a_non_bearer_scheme_answers_401() {
        assertEquals(401, statusOf("GET", "/collections/favourites/items", "Basic " + VALID_TOKEN));
    }

    @Test
    void an_item_id_wider_than_the_schema_column_answers_400() {
        String tooLongItemId = "x".repeat(129);   // item_id is VARCHAR(128)
        assertEquals(400, statusOf("PUT", "/collections/favourites/items/meme/" + tooLongItemId,
                "Bearer " + VALID_TOKEN));
        assertEquals(400, statusOf("DELETE", "/collections/favourites/items/meme/" + tooLongItemId,
                "Bearer " + VALID_TOKEN));
    }

    @Test
    void a_collection_wider_than_the_schema_column_answers_400() {
        String tooLongCollection = "c".repeat(65);   // collection is VARCHAR(64)
        assertEquals(400, statusOf("GET", "/collections/" + tooLongCollection + "/items",
                "Bearer " + VALID_TOKEN));
        assertEquals(400, statusOf("PUT", "/collections/" + tooLongCollection + "/items/meme/42",
                "Bearer " + VALID_TOKEN));
    }

    @Test
    void the_widest_fitting_item_id_is_still_accepted() {
        String widestItemId = "x".repeat(128);
        assertEquals(201, statusOf("PUT", "/collections/favourites/items/meme/" + widestItemId,
                "Bearer " + VALID_TOKEN));
    }

    private static int statusOf(String method, String path, String authorization) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .method(method, HttpRequest.BodyPublishers.noBody());
            if (authorization != null) {
                request.header("Authorization", authorization);
            }
            return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (Exception e) {
            throw new IllegalStateException(method + " " + path + " failed", e);
        }
    }
}
