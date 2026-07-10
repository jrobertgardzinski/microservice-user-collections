package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The browser's side of the cross-origin conversation, against a real server: an allowlisted
 * origin gets its echo and a fully-answered preflight (which never reaches the routes); a foreign
 * origin gets no CORS headers at all — the browser then refuses on our behalf.
 */
class CorsFilterTest {

    private WebServer server;
    private String baseUrl;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() {
        server = WebServer.builder()
                .port(0)
                .routing(routing -> routing
                        .addFilter(new CorsFilter(Set.of("http://localhost:8093")))
                        .get("/collections/favourites/items", (req, res) -> res.send("[]")))
                .build()
                .start();
        baseUrl = "http://localhost:" + server.port();
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void a_preflight_from_an_allowed_origin_is_answered_without_touching_the_routes() throws Exception {
        HttpResponse<String> preflight = http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/collections/favourites/items"))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", "http://localhost:8093")
                .header("Access-Control-Request-Method", "GET")
                .build(), HttpResponse.BodyHandlers.ofString());

        assertEquals(204, preflight.statusCode());
        assertEquals("http://localhost:8093",
                preflight.headers().firstValue("Access-Control-Allow-Origin").orElseThrow());
        assertTrue(preflight.headers().firstValue("Access-Control-Allow-Methods").orElseThrow()
                .contains("PUT"), "the UI saves with PUT, the preflight must allow it");
        assertTrue(preflight.headers().firstValue("Access-Control-Allow-Headers").orElseThrow()
                .contains("Authorization"), "the bearer token must survive the preflight");
    }

    @Test
    void an_actual_request_carries_the_origin_echo() throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/collections/favourites/items"))
                .header("Origin", "http://localhost:8093")
                .GET().build(), HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("http://localhost:8093",
                response.headers().firstValue("Access-Control-Allow-Origin").orElseThrow());
    }

    @Test
    void a_foreign_origin_gets_no_cors_headers_at_all() throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/collections/favourites/items"))
                .header("Origin", "http://evil.example.com")
                .GET().build(), HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "the API itself still answers — CORS is a browser contract");
        assertTrue(response.headers().firstValue("Access-Control-Allow-Origin").isEmpty(),
                "no echo means the browser refuses on our behalf");
    }
}
