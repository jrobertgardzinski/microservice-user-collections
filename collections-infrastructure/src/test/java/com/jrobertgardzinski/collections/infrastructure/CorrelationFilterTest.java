package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.webserver.WebServer;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The correlation id is whatever the CALLER sent, which is why it may not be taken at face value:
 * it is stamped on every log line ({@code [cid=%X{cid:-}]} in logback.xml) and echoed back. A
 * header carrying {@code "] [cid="} writes a second correlation id of the caller's choosing into
 * every line of that request — and an e-mail address of their choosing into a field the erasure
 * design deliberately keeps free of personal data. The sibling service sanitises for exactly this
 * reason; these tests pin the same rule here.
 */
@Epic("Infrastructure")
@Feature("HTTP API")
@Story("Correlation id")
class CorrelationFilterTest {

    private static WebServer server;
    private static String baseUrl;
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() {
        server = WebServer.builder()
                .port(0)
                .routing(routing -> routing
                        .addFilter(new CorrelationFilter())
                        .get("/ping", (req, res) -> res.send("pong")))
                .build()
                .start();
        baseUrl = "http://localhost:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.stop();
    }

    @Test
    void a_forged_log_line_in_the_header_is_stripped_before_anything_trusts_it() {
        String forged = "x] [cid=victim@example.com";

        String echoed = correlationIdOf(forged);

        assertFalse(echoed.contains("["), "a bracket closes this service's own log field: " + echoed);
        assertFalse(echoed.contains("@"), "an address must never reach a log field or the echo: " + echoed);
        assertEquals("xcidvictimexamplecom", echoed,
                "only [A-Za-z0-9_-] survives, so the forgery is one flat token: " + echoed);
    }

    @Test
    void a_header_longer_than_the_cap_is_cut() {
        String echoed = correlationIdOf("a".repeat(4_096));

        assertEquals(64, echoed.length(),
                "the whole header would otherwise be repeated on every log line of the request");
    }

    @Test
    void an_ordinary_correlation_id_travels_untouched() {
        assertEquals("trace-1234_abc", correlationIdOf("trace-1234_abc"),
                "sanitising must not break the ids the estate actually sends");
    }

    @Test
    void a_header_that_sanitises_to_nothing_gets_a_minted_id() {
        String echoed = correlationIdOf("!!!!");

        assertFalse(echoed.isBlank(), "a request always carries a correlation id");
        assertEquals(8, echoed.length(), "the minted one, as if no header had come: " + echoed);
    }

    private static String correlationIdOf(String inbound) {
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + "/ping"))
                            .header(CorrelationFilter.HEADER, inbound)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertTrue(response.headers().firstValue(CorrelationFilter.HEADER).isPresent(),
                    "the filter always echoes the id it used");
            return response.headers().firstValue(CorrelationFilter.HEADER).orElseThrow();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
