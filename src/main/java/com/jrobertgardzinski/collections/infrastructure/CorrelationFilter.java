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
 * (or mints a short one), puts it in the logging MDC — so every log line of this request carries
 * {@code [cid=...]} (see logback.xml) — and echoes it on the response. Helidon serves each request
 * on its own virtual thread, so the thread-local MDC is clean per request; the finally clears it.
 */
class CorrelationFilter implements Filter {

    static final String HEADER = "X-Correlation-Id";
    private static final String MDC_KEY = "cid";
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationFilter.class);

    @Override
    public void filter(FilterChain chain, RoutingRequest req, RoutingResponse res) {
        String cid = req.headers().first(HeaderNames.create(HEADER))
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
}
