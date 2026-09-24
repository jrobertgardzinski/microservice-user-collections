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

    private final ExportedObservations observations;

    MetricsEndpoint(ExportedObservations observations) {
        this.observations = observations;
    }

    void handle(ServerRequest req, ServerResponse res) {
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
     *
     * <p>The two saga lines are read from {@link ExportedObservations} rather than from statics the
     * producing classes owned. The JVM lines above them stay as they are: memory, threads and
     * uptime are properties of a process that anything can read, not facts this service knows.
     */
    String body() {
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
                + observations.recordsDropped() + "\n"
                // the GDPR line: rows this service is hiding but has not been told to erase. It
                // must fall back to zero on its own, so a gauge — see ErasureBacklogWatch
                + "# TYPE collections_erasure_backlog gauge\n"
                + "collections_erasure_backlog " + observations.erasureBacklog() + "\n"
                // the other GDPR line, and the sibling of memes' and comments' of the same name:
                // deletions this service confirmed while reserving nothing. A rise beside a steady
                // deletion rate is the address-change defect coming back
                + "# TYPE collections_saga_purge_reserved_nothing_total counter\n"
                + "collections_saga_purge_reserved_nothing_total "
                + observations.purgesReservingNothing() + "\n"
                // the third GDPR line: rows a closure had to leave under an address it erased,
                // because they were saved after the mark by a token the offline gate still
                // accepted. Nothing will ever come back for them, so this only ever rises
                + "# TYPE collections_erasure_residue_total counter\n"
                + "collections_erasure_residue_total " + observations.erasureResidue() + "\n";
    }
}
