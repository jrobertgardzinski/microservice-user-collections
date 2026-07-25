package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * This service's side of the account-deletion saga. It consumes {@code content-commands} (the
 * command microservice-security's outbox publishes), purges the leaver's collections wholesale, and
 * confirms on {@code usercollections-events} — the third participant security waits for. The purge
 * is idempotent, so at-least-once delivery needs no extra dedup. The correlation id rides the Kafka
 * header, in and out, so the async hop keeps the trace of the request that started the deletion.
 */
public class PurgeCommandsConsumer {

    static final String COMMANDS_TOPIC = "content-commands";
    static final String EVENTS_TOPIC = "usercollections-events";
    static final String CID_HEADER = "X-Correlation-Id";

    private static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 1_000;
    private static final long MAX_BACKOFF_MILLIS = 30_000;

    private static final Logger LOG = LoggerFactory.getLogger(PurgeCommandsConsumer.class);

    private final PurgeUserItems purgeUserItems;
    private final ObjectMapper mapper;
    private final long initialBackoffMillis;

    // the liveness marker /health watches: refreshed on every completed poll-handle-commit cycle
    // (and when the loop starts, so a service still warming up is not born unhealthy).
    // System.nanoTime, not currentTimeMillis: the marker measures elapsed time, and the wall
    // clock can jump (NTP step) — backwards would fake a 503, forwards would mask a real stall.
    // Package-private so the health test can age the marker without waiting out a real stall.
    volatile long lastCycleNanos = System.nanoTime();

    public PurgeCommandsConsumer(PurgeUserItems purgeUserItems, ObjectMapper mapper) {
        this(purgeUserItems, mapper, DEFAULT_INITIAL_BACKOFF_MILLIS);
    }

    /** Test seam: the loop-under-test shortens the retry backoff instead of sleeping seconds. */
    PurgeCommandsConsumer(PurgeUserItems purgeUserItems, ObjectMapper mapper,
                          long initialBackoffMillis) {
        this.purgeUserItems = purgeUserItems;
        this.mapper = mapper;
        this.initialBackoffMillis = initialBackoffMillis;
    }

    /**
     * True while the loop keeps completing cycles within the stall tolerance — the real liveness
     * behind /health: a dead or wedged consumer thread stops refreshing the marker and /health
     * turns 503. That buys visibility (the compose healthcheck marks the container unhealthy in
     * {@code docker compose ps}), not a restart — plain compose never restarts an unhealthy
     * container; an orchestrator (k3s, Swarm) acting on the same probe would.
     */
    public boolean healthy(Duration stallTolerance) {
        return System.nanoTime() - lastCycleNanos <= stallTolerance.toNanos();
    }

    /**
     * Handle one command payload: purge the user and return the confirmation to publish, or empty
     * for a command that is not ours (unknown type / malformed). Pure and broker-free, so the saga
     * scenario can drive it directly.
     */
    public Optional<String> handle(String commandPayload) {
        JsonNode command;
        try {
            command = mapper.readTree(commandPayload);
        } catch (Exception malformed) {
            LOG.warn("dropping malformed command: {}", commandPayload);
            return Optional.empty();
        }
        if (!"PURGE_USER_CONTENT".equals(command.path("type").asText())) {
            return Optional.empty();
        }
        String email = command.path("email").asText();
        String sagaId = command.path("sagaId").asText();
        if (email.isBlank()) {
            // a purge with nobody to purge: retrying can't fix it, and confirming would tell the
            // orchestrator a deletion happened that never did — so drop it without a confirmation
            LOG.warn("dropping purge command without an email (saga {})", sagaId);
            return Optional.empty();
        }
        int removed = purgeUserItems.execute(email);
        LOG.info("purged {} collection refs of {} (saga {})", removed, email, sagaId);
        try {
            return Optional.of(mapper.writeValueAsString(mapper.createObjectNode()
                    .put("type", "USER_CONTENT_PURGED")
                    .put("sagaId", sagaId)
                    .put("email", email)
                    // envelope version (workspace ADR 0004): fields only ever added within version 1
                    .put("version", 1)));
        } catch (Exception impossible) {
            throw new IllegalStateException("could not build confirmation", impossible);
        }
    }

    /**
     * The real Kafka loop: poll commands, handle each, publish the confirmation forwarding the cid,
     * then commit. Runs on a daemon virtual thread started from {@link Main} when a broker is
     * configured; absent a broker (dev, tests) it simply never runs.
     *
     * <p>The thread is this service's whole share of the saga, so it must outlive infrastructure
     * hiccups: a failed cycle (broker away, database down, produce or commit timeout) is retried
     * with backoff and WITHOUT committing — the consumer rewinds to the committed offset so the
     * batch is redelivered, which the idempotent purge absorbs. Confirmations are sent per record,
     * before the batch commits, so a failure mid-batch redelivers records whose confirmation is
     * already on the broker — those duplicates are the at-least-once contract, absorbed by the
     * offboarding side's idempotent RecordConfirmation. Only an interrupt stops the loop: Kafka's
     * own {@code InterruptException} (flag already restored by the client) and the raw
     * {@code InterruptedException} that {@code Future.get} or the backoff sleep throws (flag
     * cleared — restored here) both exit cleanly instead of being swallowed by the retry catch.
     * Malformed commands, by contrast, are dropped inside {@link #handle} and their offset
     * committed: no retry can ever fix them.
     */
    public void run(String bootstrapServers) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrapServers));
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrapServers))) {
            run(consumer, producer);
        } catch (Exception fatal) {
            // only reachable from client construction or close — a config error no retry can fix,
            // but one that must not vanish silently with the daemon thread
            LOG.error("purge consumer stopped for good", fatal);
        }
    }

    /**
     * The loop against the {@link Consumer}/{@link Producer} interfaces — the test seam: the unit
     * test drives it with kafka-clients' own MockConsumer/MockProducer while production passes the
     * real clients from {@link #run(String)}. Package-private on purpose.
     */
    void run(Consumer<String, String> consumer, Producer<String, String> producer) {
        consumer.subscribe(List.of(COMMANDS_TOPIC));
        lastCycleNanos = System.nanoTime();   // liveness counts from the loop's start
        long backoffMillis = initialBackoffMillis;
        boolean rewindNeeded = false;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (rewindNeeded) {
                    // poll() already advanced past the failed batch in memory; step back to
                    // the last committed offset so the uncommitted commands are redelivered
                    rewindToCommitted(consumer);
                    rewindNeeded = false;
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record, producer);
                }
                consumer.commitSync();
                lastCycleNanos = System.nanoTime();
                backoffMillis = initialBackoffMillis;   // a full cycle worked: forgive the past
            } catch (InterruptException stopping) {
                return;   // the JVM is going down; Kafka re-set the interrupt flag already
            } catch (InterruptedException stopping) {
                // Future.get (the confirmation ack wait) throws this with the flag CLEARED — the
                // generic catch below used to swallow it and keep the loop alive past a stop
                // request. Restore the flag for whoever joins us, and leave.
                Thread.currentThread().interrupt();
                return;
            } catch (Exception broken) {
                rewindNeeded = true;
                LOG.warn("purge consumer cycle failed, retrying uncommitted work in {} ms",
                        backoffMillis, broken);
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
            }
        }
    }

    /** Purge one command and, if it was ours, publish the confirmation before returning. */
    private void handleRecord(ConsumerRecord<String, String> record,
                              Producer<String, String> producer) throws Exception {
        String cid = header(record, CID_HEADER);
        if (cid != null) {
            MDC.put("cid", cid);   // continue the trace the deletion request started
        }
        try {
            Optional<String> confirmation = handle(record.value());
            if (confirmation.isPresent()) {
                ProducerRecord<String, String> out =
                        new ProducerRecord<>(EVENTS_TOPIC, record.key(), confirmation.get());
                if (cid != null) {
                    out.headers().add(CID_HEADER, cid.getBytes(StandardCharsets.UTF_8));
                }
                // wait for the broker's ack: fire-and-forget could lose the confirmation yet let
                // the commit below record the command as done, stalling the saga forever
                producer.send(out).get();
            }
        } finally {
            MDC.remove("cid");
        }
    }

    private static void rewindToCommitted(Consumer<String, String> consumer) {
        for (TopicPartition partition : consumer.assignment()) {
            OffsetAndMetadata committed = consumer.committed(Set.of(partition)).get(partition);
            if (committed == null) {
                consumer.seekToBeginning(Set.of(partition));   // matches auto.offset.reset=earliest
            } else {
                consumer.seek(partition, committed.offset());
            }
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "user-collections");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
