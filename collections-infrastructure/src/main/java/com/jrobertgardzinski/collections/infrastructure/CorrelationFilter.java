package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.http.HeaderNames;
import io.helidon.webserver.http.Filter;
import io.helidon.webserver.http.FilterChain;
import io.helidon.webserver.http.RoutingRequest;
import io.helidon.webserver.http.RoutingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.UUID;

/**
 * Correlation id for tracing a request across services. Reads the inbound {@code X-Correlation-Id}
 * (sanitised — see {@link #sanitize}) or mints a short one, puts it in the logging MDC — so every
 * log line of this request carries
 * {@code [cid=...]} (see logback.xml) — and echoes it on the response. Helidon serves each request
 * on its own virtual thread, so the thread-local MDC is clean per request; the finally clears it.
 */
class CorrelationFilter implements Filter {

    static final String HEADER = "X-Correlation-Id";
    private static final String MDC_KEY = "cid";
    private static final int MAX_CID_LENGTH = 64;
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationFilter.class);

    @Override
    public void filter(FilterChain chain, RoutingRequest req, RoutingResponse res) {
        // the inbound header is attacker-controlled and lands in MDC, log lines and the response
        // header — strip anything beyond [A-Za-z0-9_-] and cap the length before trusting it, the
        // same rule the comments service's filter follows. Without it a header carrying "] [cid="
        // writes a second, chosen correlation id into every log line of the request — and an
        // address of the caller's choosing into a field the erasure design keeps free of PII
        String cid = req.headers().first(HeaderNames.create(HEADER))
                .map(CorrelationFilter::sanitize)
                .filter(value -> !value.isBlank())
                .orElseGet(() -> UUID.randomUUID().toString().substring(0, 8));
        MDC.put(MDC_KEY, cid);
        res.header(HeaderNames.create(HEADER), cid);
        try {
            LOG.info("{} {}", req.prologue().method(), req.prologue().uriPath().path());
            chain.proceed();
        } finally {
            MDC.remove(MDC_KEY);
        }
    }

    /** Only {@code [A-Za-z0-9_-]}, at most {@value #MAX_CID_LENGTH} characters. */
    private static String sanitize(String cid) {
        String cleaned = cid.replaceAll("[^A-Za-z0-9_-]", "");
        return cleaned.length() > MAX_CID_LENGTH ? cleaned.substring(0, MAX_CID_LENGTH) : cleaned;
    }
}
