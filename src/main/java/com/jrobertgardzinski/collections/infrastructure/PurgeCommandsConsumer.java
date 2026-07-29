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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * This service's side of the account-deletion saga. It consumes {@code content-commands} (the
 * command microservice-security's outbox publishes), purges the leaver's collections wholesale, and
 * confirms on {@code usercollections-events} — the third participant security waits for. The purge
 * is idempotent, so at-least-once delivery needs no extra dedup. The correlation id rides the Kafka
 * header, in and out, so the async hop keeps the trace of the request that started the deletion.
 *
 * <p><b>No dead-letter queue — on purpose.</b> Malformed commands (not JSON, no e-mail) are
 * dropped and committed, because no retry can fix them. But a well-formed command whose handling
 * keeps failing (a poison pill: say, a payload that reliably crashes the store) is retried with
 * backoff forever — there is no DLQ to park it on, so it blocks its partition until a human or a
 * fix intervenes. That is the accepted cost of the at-least-once contract here: the saga must not
 * lose a purge, and the eternal retry is not silent — the cycle marker stops advancing and
 * {@code /health} (readiness) turns 503, which is exactly the alarm an operator sees. Liveness
 * ({@code /alive}) stays green through it: the loop is scheduling fine, it is the work that fails.
 */
public class PurgeCommandsConsumer {

    static final String COMMANDS_TOPIC = "content-commands";
    static final String EVENTS_TOPIC = "usercollections-events";
    static final String CID_HEADER = "X-Correlation-Id";

    /** The longest the loop legitimately pauses between iterations: the retry backoff's cap. */
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /**
     * The producer's delivery clocks, set EXPLICITLY because /alive depends on them (the same
     * discipline as microservice-offboarding's KafkaLoop): the confirmation's {@code send().get()}
     * during a broker outage blocks a loop iteration for up to {@code delivery.timeout.ms} — with
     * Kafka's default of 120s this ONE block would be four times the next-largest term in
     * {@link Main#WORST_ITERATION} and would drag the derived floor past four minutes, so a mere
     * broker outage would either read as a dead thread or force an absurd tolerance. 30s bounds the block
     * well inside the tolerance; {@link Main#ALIVE_STALL_FLOOR} is derived from these same
     * constants so the two can never drift apart.
     */
    static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    /**
     * One in-flight request's timeout, producer AND consumer: two of these fit inside
     * {@link #DELIVERY_TIMEOUT}, and one inside {@link #API_TIMEOUT}.
     */
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);
    /** How long send() may block on metadata — the same bound as the delivery timeout. */
    static final Duration MAX_BLOCK = Duration.ofSeconds(30);

    /**
     * The CONSUMER's blocking clock, set EXPLICITLY for exactly the reason the producer's are:
     * {@code commitSync()}, the rewind's {@code committed()} lookup and the broker probe below
     * all wait up to {@code default.api.timeout.ms} on an unresponsive broker, and Kafka leaves
     * that at 60s. One such call would already outlast half the /alive tolerance and two would
     * break it, turning a broker outage into a "dead thread" restart — the very thing the
     * producer's clocks were pinned down to prevent. 20s per API call (over 15s per in-flight
     * request) bounds them well inside the tolerance, and {@link Main#ALIVE_STALL_FLOOR} is
     * derived from this constant too, so the two cannot drift apart.
     */
    static final Duration API_TIMEOUT = Duration.ofSeconds(20);

    /** One poll's wait — also part of the /alive floor arithmetic in {@link Main}. */
    static final Duration POLL_EVERY = Duration.ofSeconds(1);

    /**
     * How many commands one poll may hand back. Kafka's default is 500, and this loop confirms
     * each record SYNCHRONOUSLY — {@code send().get()} inside the record loop — so the cost of a
     * batch is O(N) round trips, not one. That is affordable while the broker answers in
     * milliseconds; it stops being affordable exactly when it matters. After a longer outage the
     * first poll drains the whole backlog, and 500 records against a broker that is slow rather
     * than absent can walk the iteration past {@code max.poll.interval.ms} (Kafka's default,
     * 300s) — the group then considers this member gone, rebalances, and the {@code commitSync()}
     * at the end of the cycle fails with CommitFailedException. The retry path rewinds and
     * re-handles the SAME oversized batch, which takes just as long, which times out again: a
     * livelock, healed only by a human. 50 keeps a drained backlog to batches the loop can finish
     * and commit, at the price of more polls — and more polls is precisely what a recovering
     * consumer wants, since each one refreshes the liveness beat and the readiness marker.
     *
     * <p>The cap is about the DEGRADED-but-answering broker. It is deliberately NOT a term in
     * {@link Main#ALIVE_STALL_FLOOR}: when the broker is truly silent the FIRST record's
     * {@code send().get()} spends one {@link #DELIVERY_TIMEOUT} and throws straight out of the
     * record loop, so a failing iteration pays one send, never fifty (the same argument that lets
     * the floor count one database block rather than one per record).
     */
    static final int MAX_POLL_RECORDS = 50;

    /**
     * The /health honesty probe's cadence and patience: an EMPTY poll against a DEAD broker
     * returns normally, so an idle consumer would keep "completing" cycles and /health would
     * stay 200 through an outage the javadoc promises it reports. The loop therefore asks the
     * broker something that requires an ANSWER — the metadata of the ONE topic it consumes
     * ({@code partitionsFor(COMMANDS_TOPIC)}, not the whole cluster's topic list) — on its first
     * iteration and then at most once per {@code PROBE_EVERY}. The cadence is on the CLOCK, not
     * on a cycle count, on purpose: with commands flowing a cycle takes milliseconds, and "every
     * N cycles" would fire this round trip several times a second for nothing.
     *
     * <p>No answer within {@code PROBE_TIMEOUT} fails the cycle — and ONLY the cycle: nothing was
     * consumed, so nothing is rewound (see the {@link BrokerSilent} catch in {@link #run}). The
     * readiness marker freezes, /health turns 503, and the probe is retried every iteration; the
     * retry backoff is what stretches noticing the broker's RETURN to at most one
     * {@link #MAX_BACKOFF} (30s) after it starts answering again.
     */
    static final Duration PROBE_EVERY = Duration.ofSeconds(10);
    static final Duration PROBE_TIMEOUT = Duration.ofSeconds(5);

    private static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 1_000;
    private static final long MAX_BACKOFF_MILLIS = MAX_BACKOFF.toMillis();

    private static final Logger LOG = LoggerFactory.getLogger(PurgeCommandsConsumer.class);

    private final PurgeUserItems purgeUserItems;
    private final ObjectMapper mapper;
    private final long initialBackoffMillis;

    // the readiness marker /health watches: refreshed on every completed poll-handle-commit cycle
    // (and when the loop starts, so a service still warming up is not born unhealthy).
    // System.nanoTime, not currentTimeMillis: the marker measures elapsed time, and the wall
    // clock can jump (NTP step) — backwards would fake a 503, forwards would mask a real stall.
    // Package-private so the health test can age the marker without waiting out a real stall.
    volatile long lastCycleNanos = System.nanoTime();

    // the liveness marker /alive watches: refreshed at the TOP of every loop iteration —
    // successful, failing and backoff ones alike — so it distinguishes "the thread still
    // schedules, the work fails" (alive, not ready) from "the thread is gone or wedged" (not
    // alive). Same monotonic clock and the same package-private test seam as above.
    volatile long lastScheduledNanos = System.nanoTime();

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
     * READINESS, behind /health: true while the loop keeps COMPLETING cycles within the stall
     * tolerance. Cycles stop completing when a dependency is broken — database down, broker
     * unreachable, a poison pill in eternal retry — so /health turning 503 means "this instance
     * cannot currently do its saga share", whether or not the thread itself is fine. "Broker
     * unreachable" is honest even on a QUIET topic: empty polls return normally against a dead
     * broker, so the loop backs its cycles with a periodic round-trip probe (see
     * {@link #PROBE_EVERY}) — a broker that stops answering fails the probing cycle
     * within {@code PROBE_EVERY} plus {@code PROBE_TIMEOUT}. That cadence is PART of the
     * detection time, so the configured tolerance is not the whole promise: a broker that dies
     * the instant AFTER a successful probe leaves the marker moving for up to one
     * {@code PROBE_EVERY} (10s) before the next probe even asks, plus its {@code PROBE_TIMEOUT}
     * (5s) — a 60s {@code COLLECTIONS_CONSUMER_STALL_SEC} really means "noticed within about
     * 75s". Read the env as the tolerance it is, not as a detection deadline. That buys
     * visibility (the compose healthcheck marks the container unhealthy in
     * {@code docker compose ps}), not a restart — plain compose never restarts an unhealthy
     * container; an orchestrator (k3s, Swarm) acting on the same probe would.
     */
    public boolean healthy(Duration stallTolerance) {
        return System.nanoTime() - lastCycleNanos <= stallTolerance.toNanos();
    }

    /**
     * LIVENESS, behind /alive: true while the loop thread keeps getting scheduled at all — the
     * marker is refreshed at the top of every iteration, failing and backoff ones included, so a
     * database outage (cycles fail, thread loops on) keeps /alive at 200 while /health reports
     * the stall. Only a thread that has really stopped — exited, or wedged inside one iteration
     * longer than the tolerance — turns /alive into a 503. The split is deliberate: restarting
     * on /alive can heal a wedged process, restarting on a broken dependency (/health) would
     * just crash-loop without fixing the dependency.
     */
    public boolean alive(Duration stallTolerance) {
        return System.nanoTime() - lastScheduledNanos <= stallTolerance.toNanos();
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
            // NOT the payload itself: a purge command carries the leaver's e-mail, and even a
            // malformed one may — PII stays out of the logs, the size is enough to investigate
            // (the same rule the comments service's listener follows)
            LOG.warn("dropping a malformed command ({} chars, not valid JSON)",
                    commandPayload == null ? 0 : commandPayload.length());
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
        // the saga id identifies the run in logs; the e-mail is PII and stays out of INFO lines
        LOG.info("purged {} collection refs of one leaver (saga {})", removed, sagaId);
        try {
            var confirmation = mapper.createObjectNode()
                    .put("type", "USER_CONTENT_PURGED")
                    .put("email", email)
                    // envelope version (workspace ADR 0004): fields only ever added within version 1
                    .put("version", 1);
            // A BLANK sagaId is worse than an absent one. The orchestrator drops a confirmation
            // whose sagaId is present but unparseable — a deliberate poison-pill rule — while one
            // with NO sagaId falls back to matching by e-mail. So a purge command that arrived
            // without the field had its confirmation thrown away rather than matched, and the saga
            // waited out its timeout for an answer that had in fact come back.
            if (sagaId != null && !sagaId.isBlank()) {
                confirmation.put("sagaId", sagaId);
            }
            return Optional.of(mapper.writeValueAsString(confirmation));
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
        lastCycleNanos = System.nanoTime();   // readiness counts from the loop's start
        long backoffMillis = initialBackoffMillis;
        boolean rewindNeeded = false;
        // the first iteration probes at once — a broker that is already gone must not need a
        // cadence's grace before /health says so. Nanotime DIFFERENCES only, never absolutes
        long nextProbeNanos = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            // liveness first: the marker moves on EVERY iteration the scheduler grants us —
            // including the ones that will fail and back off — so /alive tracks the thread,
            // not the luck of the work
            lastScheduledNanos = System.nanoTime();
            try {
                if (rewindNeeded) {
                    // poll() already advanced past the failed batch in memory; step back to
                    // the last committed offset so the uncommitted commands are redelivered
                    rewindToCommitted(consumer);
                    rewindNeeded = false;
                }
                if (System.nanoTime() - nextProbeNanos >= 0) {
                    // the honesty probe (see PROBE_EVERY): empty polls prove nothing about the
                    // broker, this round trip does. The next probe is scheduled only on SUCCESS,
                    // so a failing one is retried every iteration until the broker answers
                    probeBroker(consumer);
                    nextProbeNanos = System.nanoTime() + PROBE_EVERY.toNanos();
                }
                ConsumerRecords<String, String> records = consumer.poll(POLL_EVERY);
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
            } catch (BrokerSilent silent) {
                // the probe found nobody home. Unlike every other failure NOTHING was consumed
                // here, so there is nothing to rewind — and setting the rewind flag would make
                // the NEXT iteration walk committed() over the assignment (up to one
                // default.api.timeout.ms) for not a single record's worth of gain, stretching
                // the iteration, and with it the gap between two liveness beats, for nothing.
                // Freeze readiness, back off, ask again.
                LOG.warn("purge consumer got no answer from the broker, probing again in {} ms"
                        + " (readiness stalls until it does)", backoffMillis, silent.getCause());
                if (!backedOff(backoffMillis)) {
                    return;
                }
                backoffMillis = grown(backoffMillis);
            } catch (Exception broken) {
                rewindNeeded = true;
                LOG.warn("purge consumer cycle failed, retrying uncommitted work in {} ms",
                        backoffMillis, broken);
                if (!backedOff(backoffMillis)) {
                    return;
                }
                backoffMillis = grown(backoffMillis);
            }
        }
    }

    /**
     * The honesty probe itself: ONE metadata round trip, for the single topic this service
     * consumes — {@code listTopics} would ask for every topic in the cluster, a needlessly fat
     * answer for a question this narrow. Any failure becomes {@link BrokerSilent} so the loop can
     * tell "the broker did not answer" (nothing consumed, nothing to rewind) from "handling
     * failed" (a batch is in flight and must be redelivered); a stop request rides through
     * untouched, or the shutdown would be reported as a broker problem.
     *
     * <p>The interrupt is the ONLY stop signal here — nothing ever calls {@code wakeup()} on this
     * consumer — so {@link InterruptException} is the only shield this probe needs. The mirror
     * image of microservice-offboarding's KafkaLoop, whose shutdown() sends BOTH a wakeup and an
     * interrupt and whose probe therefore shields both; between them the pair is now symmetric,
     * each shielding exactly the signals its own shutdown can deliver.
     */
    private static void probeBroker(Consumer<String, String> consumer) {
        try {
            consumer.partitionsFor(COMMANDS_TOPIC, PROBE_TIMEOUT);
        } catch (InterruptException stopping) {
            throw stopping;
        } catch (Exception unanswered) {
            throw new BrokerSilent(unanswered);
        }
    }

    /** The broker did not answer the probe — a failure with NO consumed batch behind it. */
    private static final class BrokerSilent extends RuntimeException {
        BrokerSilent(Throwable cause) {
            super("the broker did not answer the readiness probe within " + PROBE_TIMEOUT, cause);
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

    /** One second doubling to {@link #MAX_BACKOFF}, reset by any completed cycle. */
    private static long grown(long backoffMillis) {
        return Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
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

    /**
     * A failed cycle must not lose its records: poll() already advanced the in-memory position
     * past them, so step every assigned partition back to its committed offset (or the beginning,
     * matching auto.offset.reset=earliest) before retrying.
     *
     * <p>ONE bulk {@code committed(assignment)} call, never one per partition (the shape
     * microservice-offboarding's KafkaLoop has always had): each such lookup waits up to
     * {@code default.api.timeout.ms} on an unresponsive broker, so the per-partition loop turned a
     * rewind on an N-partition topic into N of those waits inside a SINGLE iteration — no liveness
     * beat for N times the clock, which is how a plain broker outage could walk past the /alive
     * tolerance and earn a restart that fixes nothing.
     */
    private static void rewindToCommitted(Consumer<String, String> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        for (TopicPartition partition : assignment) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset == null) {
                consumer.seekToBeginning(Set.of(partition));   // matches auto.offset.reset=earliest
            } else {
                consumer.seek(partition, offset.offset());
            }
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Package-private so the test can pin the clocks /alive's floor is derived from. */
    static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "user-collections");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // the CONSUMER's clocks, EXPLICIT because /alive depends on them just as much as on the
        // producer's (see API_TIMEOUT): commitSync(), the rewind's committed() lookup and the
        // readiness probe each block up to default.api.timeout.ms, and Kafka's 60s default would
        // let a single one of them eat most of the stall tolerance — the floor Main derives is
        // computed from this very constant
        props.put("default.api.timeout.ms", String.valueOf(API_TIMEOUT.toMillis()));
        props.put("request.timeout.ms", String.valueOf(REQUEST_TIMEOUT.toMillis()));
        // the batch's size, EXPLICIT because this loop pays its confirmation ack PER RECORD
        // (send().get() inside the record loop, see handleRecord) — see MAX_POLL_RECORDS
        props.put("max.poll.records", String.valueOf(MAX_POLL_RECORDS));
        return props;
    }

    private static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // the delivery clocks, EXPLICIT because /alive depends on them (see DELIVERY_TIMEOUT):
        // during a broker outage the confirmation's send().get() blocks an iteration for up to
        // delivery.timeout.ms and a metadata-less send() for up to max.block.ms — both must stay
        // well inside COLLECTIONS_ALIVE_STALL_SEC, whose floor Main derives from these very
        // constants. Kafka's defaults (120s / 60s) would let one blocked iteration outlast the
        // probe and turn a broker outage into a false "dead thread" restart
        props.put("delivery.timeout.ms", String.valueOf(DELIVERY_TIMEOUT.toMillis()));
        props.put("request.timeout.ms", String.valueOf(REQUEST_TIMEOUT.toMillis()));
        props.put("max.block.ms", String.valueOf(MAX_BLOCK.toMillis()));
        return props;
    }
}
