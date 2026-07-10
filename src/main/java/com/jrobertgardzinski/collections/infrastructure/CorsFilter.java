package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.Filter;
import io.helidon.webserver.http.FilterChain;
import io.helidon.webserver.http.RoutingRequest;
import io.helidon.webserver.http.RoutingResponse;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CORS for the browser UI (collections-ui runs on its own origin — that is the point: the
 * cross-origin request is what exercises this). Hand-rolled like the rest of this service's
 * edge — an allowlisted {@code Origin} gets its echo and a preflight ({@code OPTIONS} with a
 * requested method) is answered here without touching the routes; any other origin gets no CORS
 * headers at all, which is the browser-side equivalent of a refusal. The allowlist comes from
 * {@code COLLECTIONS_ALLOWED_ORIGINS} (comma-separated); with none set, the UI's compose origin
 * and the Vite dev server are allowed — matching security's own dev-friendly CORS defaults.
 */
final class CorsFilter implements Filter {

    // the favourites UI (8093 + its dev server 5173) and the meme gallery (8083), whose star
    // button saves/removes favourites straight from the browser
    private static final String DEFAULT_ORIGINS =
            "http://localhost:8093,http://localhost:5173,http://localhost:8083";

    private final Set<String> allowedOrigins;

    CorsFilter(Set<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    static CorsFilter fromEnv(String csv) {
        String origins = csv == null || csv.isBlank() ? DEFAULT_ORIGINS : csv;
        return new CorsFilter(Arrays.stream(origins.split(","))
                .map(String::trim).filter(origin -> !origin.isEmpty())
                .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public void filter(FilterChain chain, RoutingRequest req, RoutingResponse res) {
        Optional<String> origin = req.headers().first(HeaderNames.create("Origin"))
                .filter(allowedOrigins::contains);
        if (origin.isPresent()) {
            res.header(HeaderNames.create("Access-Control-Allow-Origin"), origin.get());
            res.header(HeaderNames.create("Vary"), "Origin");
            if ("OPTIONS".equals(req.prologue().method().text())
                    && req.headers().first(HeaderNames.create("Access-Control-Request-Method")).isPresent()) {
                res.header(HeaderNames.create("Access-Control-Allow-Methods"), "GET, PUT, DELETE, OPTIONS");
                res.header(HeaderNames.create("Access-Control-Allow-Headers"),
                        "Authorization, Content-Type, X-Correlation-Id");
                res.header(HeaderNames.create("Access-Control-Max-Age"), "3600");
                res.status(Status.NO_CONTENT_204).send();
                return;   // a preflight never reaches the routes
            }
        }
        chain.proceed();
    }
}
