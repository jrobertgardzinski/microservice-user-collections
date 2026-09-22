package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * What this service does about somebody changing their e-mail address: it moves their saved
 * references onto the new one ({@link RekeyUserItems}).
 *
 * <p>The address is not an identifier here, it is a NAME that identity happens to use as one —
 * every row is keyed by the token's {@code sub}. Until this loop existed a confirmed address change
 * emptied a member's lists without a word, and their later deletion reserved nothing while this
 * service confirmed the erasure anyway.
 *
 * <p>{@code security-events} is the facts topic, so most of what arrives on it is somebody else's
 * business — {@code ACCOUNT_DELETION_REQUESTED} belongs to microservice-offboarding, which turns it
 * into the purge commands {@link PurgeCommandsConsumer} answers. Everything but
 * {@code EMAIL_CHANGED} is ignored WITHOUT a word: a fact this service has no use for is not an
 * anomaly, and a log line per deletion request in the portal would be noise that teaches an
 * operator to stop reading.
 *
 * <h2>A third loop, and which of the two existing ones it takes after</h2>
 *
 * This service consumes Kafka by hand, so none of the wiring the two Spring siblings got from an
 * annotation comes for free — every rule below is a decision rather than a default.
 *
 * <p>It is the SAGA consumer's rules, not the cascade's, in the one place they differ: a failed
 * cycle is rewound and retried without end, and no record is ever given up on. {@link
 * CascadeConsumer} may abandon an event after {@link CascadeConsumer#MAX_ATTEMPTS} because its worst
 * case is a row pointing at a meme that no longer exists, which the UI already renders as exactly
 * that. The worst case HERE is the defect this class was written to close: a rename dropped is a
 * member whose lists read empty for ever and whose erasure confirms nothing, and no later fact
 * repeats it.
 *
 * <p>It is watched by BOTH probes, which the cascade deliberately is not, for the same reason: a
 * stalled cascade is cleanup debt, while a stalled rename means this instance is answering reads
 * with the wrong person's lists. Its markers are the saga consumer's, move at the same two moments,
 * and are read with the same two tolerances — and its worst legal iteration (a rewind lookup, a
 * poll, one database block, a commit and a backoff) is strictly shorter than the one {@link
 * Main#WORST_ITERATION} sums, because there is no confirmation to send and no broker probe, so the
 * floors derived there cover this loop as they stand.
 *
 * <p>Its own consumer group, like the cascade's: three threads in one group subscribing to
 * different topics would fight over the assignment. {@code auto.offset.reset=earliest} matters more
 * here than anywhere else in this service — a group with no committed offset must read the renames
 * announced before it existed rather than skip them, and re-keying them all is free.
 */
public class SecurityEventsConsumer {

    /** The facts topic microservice-security publishes on; the twin literal is its outbox's. */
    static final String TOPIC = "security-events";

    /** Its OWN group, for the reason {@link CascadeConsumer#GROUP_ID} gives. */
    static final String GROUP_ID = "user-collections-security-events";

    /** The one fact on this topic this service acts on. */
    static final String EMAIL_CHANGED = "EMAIL_CHANGED";

    static final String CID_HEADER = "X-Correlation-Id";

    /**
     * The clocks, taken from {@link PurgeCommandsConsumer} rather than spelled again: {@link
     * Main#ALIVE_STALL_FLOOR} is arithmetic over those very constants, and this loop now stands
     * behind the same probe. A private copy here would be a second set of numbers the floor does
     * not know about — which is how a tolerance stops describing what it measures.
     */
    static final Duration POLL_EVERY = PurgeCommandsConsumer.POLL_EVERY;
    static final Duration API_TIMEOUT = PurgeCommandsConsumer.API_TIMEOUT;
    static final Duration REQUEST_TIMEOUT = PurgeCommandsConsumer.REQUEST_TIMEOUT;

    /**
     * How many facts one poll may hand back. This loop pays no broker round trip per record (there
     * is nothing to confirm), so a batch costs one database statement each — but the ceiling matters
     * for the reason it does everywhere here: after an outage the first poll drains the backlog, and
     * a batch longer than {@code max.poll.interval.ms} gets the member evicted mid-batch.
     */
    static final int MAX_POLL_RECORDS = 50;

    private static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 1_000;
    private static final long MAX_BACKOFF_MILLIS = PurgeCommandsConsumer.MAX_BACKOFF.toMillis();

    private static final Logger LOG = LoggerFactory.getLogger(SecurityEventsConsumer.class);

    private final RekeyUserItems rekeyUserItems;
    private final ObjectMapper mapper;
    private final long initialBackoffMillis;

    // the two probe markers, with the same meanings and the same monotonic clock as the saga
    // consumer's: readiness moves when a record finishes and when a cycle completes, liveness at
    // the top of every iteration, failing and backoff ones included. Package-private so the test
    // can age them without waiting out a real stall
    volatile long lastCycleNanos = System.nanoTime();
    volatile long lastScheduledNanos = System.nanoTime();

    public SecurityEventsConsumer(RekeyUserItems rekeyUserItems, ObjectMapper mapper) {
        this(rekeyUserItems, mapper, DEFAULT_INITIAL_BACKOFF_MILLIS);
    }

    /** Test seam: the loop-under-test retries in milliseconds instead of sleeping out seconds. */
    SecurityEventsConsumer(RekeyUserItems rekeyUserItems, ObjectMapper mapper,
                           long initialBackoffMillis) {
        this.rekeyUserItems = rekeyUserItems;
        this.mapper = mapper;
        this.initialBackoffMillis = initialBackoffMillis;
    }

    /** Readiness: nothing has finished for longer than the tolerance. */
    public boolean healthy(Duration stallTolerance) {
        return System.nanoTime() - lastCycleNanos <= stallTolerance.toNanos();
    }

    /** Liveness: the loop thread itself has stopped being scheduled. */
    public boolean alive(Duration stallTolerance) {
        return System.nanoTime() - lastScheduledNanos <= stallTolerance.toNanos();
    }

    /**
     * Handle one fact: re-key whoever moved and return how many rows went with them. Pure and
     * broker-free, so the tests drive the whole contract without a Kafka in sight.
     *
     * <p>Returns 0 for everything that is not ours and for every poison pill; the only thing that
     * escapes is a genuine store failure, and the loop is what decides how to treat that.
     */
    public int handle(String payload) {
        JsonNode fact;
        try {
            fact = mapper.readTree(payload);
        } catch (Exception malformed) {
            // NOT the payload itself: every fact on this topic carries somebody's address, and a
            // malformed one may — the same rule the purge consumer follows, for the same reason
            LOG.warn("dropping a malformed security fact ({} chars, not valid JSON)",
                    payload == null ? 0 : payload.length());
            return 0;
        }
        if (!EMAIL_CHANGED.equals(fact.path("type").asText())) {
            return 0;
        }
        String oldEmail = fact.path("oldEmail").asText();
        // the NEW address, under the name every fact on this topic uses for its subject's address —
        // so the intuitive reading of "email" here is precisely the wrong one, and a consumer that
        // swapped the two would move everyone onto the address they have just left
        String newEmail = fact.path("email").asText();
        if (oldEmail.isBlank() || newEmail.isBlank()) {
            // a rename missing either end cannot be carried out and cannot be retried into
            // existence; the id is derived from the two addresses, so it is what an investigation
            // has to go on
            LOG.warn("dropping an {} without both addresses (fact {})", EMAIL_CHANGED, factId(fact));
            return 0;
        }
        int moved = rekeyUserItems.execute(oldEmail, newEmail);
        // the fact id, never the addresses: this line reports a person's identity changing and the
        // log has no retention anybody here controls. The id is derived from the pair, so the same
        // rename always reads the same — which is also what makes "0 moved" recognisable as the
        // redelivery it usually is
        LOG.info("re-keyed {} saved references onto a member's new address (fact {})",
                moved, factId(fact));
        return moved;
    }

    private static String factId(JsonNode fact) {
        String id = fact.path("id").asText();
        return id.isBlank() ? "no id" : id;
    }

    /**
     * The real Kafka loop, on a daemon virtual thread started from {@link Main} when a broker is
     * configured; absent a broker (dev, tests) it simply never runs.
     */
    public void run(String bootstrapServers) {
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps(bootstrapServers))) {
            run(consumer);
            // the stop interrupt has done its job; clear the flag so close() can still leave the
            // group cleanly instead of throwing InterruptException at its first blocking call
            // (the same ending both other loops have, for the same reason)
            Thread.interrupted();
        } catch (Exception fatal) {
            // client construction or close: a config error no retry can fix, but one that must
            // not vanish silently with the daemon thread
            LOG.error("security events consumer stopped for good", fatal);
        }
    }

    /**
     * The loop against the {@link Consumer} interface — the test seam, driven by kafka-clients' own
     * MockConsumer while production passes the real client from {@link #run(String)}.
     *
     * <p>The saga loop's skeleton with the saga-specific parts taken out: no producer, no broker
     * probe and no record budget, because there is nothing to confirm, nobody is blocked on an
     * answer, and no rename may ever be dropped. What remains — rewind the failed batch, back off,
     * try again, for ever — is what makes the re-key a promise rather than an attempt.
     */
    void run(Consumer<String, String> consumer) {
        consumer.subscribe(List.of(TOPIC));
        LOG.info("security events consumer listening on {} as group {}", TOPIC, GROUP_ID);
        lastCycleNanos = System.nanoTime();   // readiness counts from the loop's start
        long backoffMillis = initialBackoffMillis;
        boolean rewindNeeded = false;
        while (!Thread.currentThread().isInterrupted()) {
            // liveness first: the marker moves on EVERY iteration the scheduler grants us,
            // including the ones that will fail and back off
            lastScheduledNanos = System.nanoTime();
            try {
                if (rewindNeeded) {
                    // poll() already advanced past the failed batch in memory; step back to the
                    // last committed offset so the unhandled renames are redelivered
                    PurgeCommandsConsumer.rewindToCommitted(consumer);
                    rewindNeeded = false;
                }
                ConsumerRecords<String, String> records = consumer.poll(POLL_EVERY);
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record);
                    lastCycleNanos = System.nanoTime();   // readiness counts records, not batches
                }
                consumer.commitSync();
                lastCycleNanos = System.nanoTime();
                backoffMillis = initialBackoffMillis;   // a full cycle worked: forgive the past
            } catch (InterruptException stopping) {
                return;   // the JVM is going down; Kafka re-set the interrupt flag already
            } catch (Exception broken) {
                rewindNeeded = true;
                LOG.warn("security events cycle failed, retrying uncommitted work in {} ms",
                        backoffMillis, broken);
                if (!backedOff(backoffMillis)) {
                    return;
                }
                backoffMillis = grown(backoffMillis);
            }
        }
    }

    /** One record: correlation id into the MDC, then the re-key. */
    private void handleRecord(ConsumerRecord<String, String> record) {
        String cid = header(record, CID_HEADER);
        if (cid != null) {
            MDC.put("cid", cid);   // continue the trace the address change started in security
        }
        try {
            handle(record.value());
        } finally {
            MDC.remove("cid");
        }
    }

    /** Sleep the backoff; false when the stop interrupt cut it short (flag already restored). */
    private static boolean backedOff(long backoffMillis) {
        try {
            Thread.sleep(backoffMillis);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** One second doubling to the saga loop's ceiling, reset by any completed cycle. */
    private static long grown(long backoffMillis) {
        return Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Package-private so the test can pin the group and the clocks. */
    static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "false");
        // earliest, and here it is load-bearing rather than merely sensible: this service has been
        // keying rows by an address for longer than it has been listening for renames, so the first
        // run walks the retained history of the topic and catches up every member who has already
        // moved. Every re-key is idempotent, so replaying them costs nothing
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("default.api.timeout.ms", String.valueOf(API_TIMEOUT.toMillis()));
        props.put("request.timeout.ms", String.valueOf(REQUEST_TIMEOUT.toMillis()));
        props.put("max.poll.records", String.valueOf(MAX_POLL_RECORDS));
        return props;
    }
}
