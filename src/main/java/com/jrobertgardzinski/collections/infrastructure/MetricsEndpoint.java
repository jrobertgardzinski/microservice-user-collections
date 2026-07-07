package com.jrobertgardzinski.collections.infrastructure;

import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.lang.management.ManagementFactory;

/**
 * The service's vitals in Prometheus text format at {@code /metrics}, scraped by the workspace's
 * Prometheus (job "user-collections"). Hand-rolled to the house's lean taste, matching the manual
 * exporters in the formula backend — the JVM's basics and uptime. No registry dependency; the
 * format is a handful of lines of convention. (Prometheus's own {@code up} tells you it is alive.)
 */
final class MetricsEndpoint {

    private static final long STARTED = System.currentTimeMillis();

    private MetricsEndpoint() {
    }

    static void handle(ServerRequest req, ServerResponse res) {
        Runtime rt = Runtime.getRuntime();
        String body = "# TYPE collections_jvm_memory_used_bytes gauge\n"
                + "collections_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE collections_jvm_threads gauge\n"
                + "collections_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE collections_uptime_seconds gauge\n"
                + "collections_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n";
        res.send(body);
    }
}
