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
        res.send(body());
    }

    /**
     * The exposition text, as a plain function so a test can read it without a running WebServer.
     *
     * <p>{@code collections_kafka_records_dropped_total} is the one line an operator alerts on: the
     * sibling of comments' {@code comments_kafka_records_dropped_total}, incremented once per saga
     * record this instance abandoned after its retry budget (see
     * {@link PurgeCommandsConsumer#RETRY_BUDGET}). One increment means one account deletion this
     * service did not finish — the saga is about to compensate, and nobody else will say so.
     */
    static String body() {
        Runtime rt = Runtime.getRuntime();
        return "# TYPE collections_jvm_memory_used_bytes gauge\n"
                + "collections_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE collections_jvm_threads gauge\n"
                + "collections_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE collections_uptime_seconds gauge\n"
                + "collections_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n"
                + "# TYPE collections_kafka_records_dropped_total counter\n"
                + "collections_kafka_records_dropped_total{topic=\""
                + PurgeCommandsConsumer.COMMANDS_TOPIC + "\"} "
                + PurgeCommandsConsumer.recordsDropped() + "\n"
                // the GDPR line: rows this service is hiding but has not been told to erase. It
                // must fall back to zero on its own, so a gauge — see ErasureBacklogWatch
                + "# TYPE collections_erasure_backlog gauge\n"
                + "collections_erasure_backlog " + ErasureBacklogWatch.backlog() + "\n";
    }
}
