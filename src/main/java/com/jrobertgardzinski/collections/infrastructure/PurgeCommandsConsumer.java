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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This service's side of the account-deletion saga. It consumes {@code content-commands} (the
 * command microservice-security's outbox publishes), purges the leaver's collections wholesale, and
 * confirms on {@code usercollections-events} — the third participant security waits for. The purge
 * is idempotent, so at-least-once delivery needs no extra dedup. The correlation id rides the Kafka
 * header, in and out, so the async hop keeps the trace of the request that started the deletion.
 *
 * <p><b>No dead-letter queue — on purpose.</b> Malformed commands (not JSON, no e-mail) are
 * dropped and committed, because no retry can fix them. A well-formed command whose handling keeps
 * failing (the store away, a poison pill payload) is retried with backoff — but only for as long as
 * {@link #RETRY_BUDGET}, after which THAT record is abandoned: its offset is committed, the drop is
 * logged as an ERROR and counted in {@code collections_kafka_records_dropped_total}. There is no DLQ
 * to park it on, so an abandoned purge is the saga's problem again — which is exactly right, because
 * the orchestrator's patience is finite (see {@link #RETRY_BUDGET}) and a purge that lands after the
 * saga compensated erases the collections of an account the leaver has been told he still owns.
 * Unbounded retrying survives only where NOTHING was consumed — a broker that will not answer the
 * probe, a poll or a commit that fails — because there no record's clock is running.
 *
 * <p>The retrying is not silent either: while it lasts the cycle marker stops advancing and
 * {@code /health} (readiness) turns 503, which is the alarm an operator sees. Liveness
 * ({@code /alive}) stays green through it: the loop is scheduling fine, it is the work that fails.
 */
public class PurgeCommandsConsumer {

    static final String COMMANDS_TOPIC = "content-commands";
    static final String EVENTS_TOPIC = "usercollections-events";
    static final String CID_HEADER = "X-Correlation-Id";

    /** The longest the loop legitimately pauses between iterations: the retry backoff's cap. */
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /**
     * How long ONE record may be retried before it is abandoned — a wall-clock deadline, not a count
     * of attempts. Ported from microservice-comments' {@code SagaRetryBudget}, whose javadoc argues
     * the arithmetic in full and which names this service as the participant that could not simply
     * copy the eternal retry. The reason is the orchestrator's finite patience:
     *
     * <ul>
     *   <li>{@code OFFBOARDING_PURGE_TIMEOUT_SEC} = 120s — a saga unconfirmed that long is overdue;</li>
     *   <li>the sweeper wakes every 15s and re-commands while retries remain (3), so the re-commands
     *       land at ≈120s, ≈135s and ≈150s;</li>
     *   <li>at ≈165s the retries are spent: the saga compensates, microservice-security hands the
     *       account back and mails the leaver that the deletion FAILED.</li>
     * </ul>
     *
     * <p>A participant that retried without end would purge whenever its store came back — half an
     * hour later, a day later — erasing the collections of an account the saga has already restored
     * to its owner, with no signal to him or to an operator. So the retrying is bounded, and 90s is
     * the same bound the other two participants carry, so that all three share one budget: long
     * enough for a real second attempt (a blocked Postgres call can spend a whole 30s connection
     * timeout before it even throws) and short enough to end before the sweeper's first re-command at
     * ≈120s, which is the moment this record stops being the one in charge of that purge.
     */
    static final Duration RETRY_BUDGET = Duration.ofSeconds(90);

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

    /**
     * How many records this process abandoned after their {@link #RETRY_BUDGET} — the counter behind
     * {@code collections_kafka_records_dropped_total} in {@link MetricsEndpoint}, the sibling of
     * comments' {@code comments_kafka_records_dropped_total}. One increment means one account
     * deletion this service did not finish, and the saga is about to compensate: without it the
     * bounded retry would trade a silent late purge for a silent lost one, which is no better. Static
     * because the exporter is a plain function of the process (no registry in this service) and a
     * Prometheus counter is per process anyway.
     */
    private static final AtomicLong RECORDS_DROPPED = new AtomicLong();

    private final PurgeUserItems purgeUserItems;
    private final ObjectMapper mapper;
    private final long initialBackoffMillis;
    private final Duration retryBudget;

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
        this(purgeUserItems, mapper, initialBackoffMillis, RETRY_BUDGET);
    }

    /**
     * Test seam: the loop-under-test also shortens the per-record budget, so the drop can be observed
     * in milliseconds instead of sitting out 90 real seconds.
     */
    PurgeCommandsConsumer(PurgeUserItems purgeUserItems, ObjectMapper mapper,
                          long initialBackoffMillis, Duration retryBudget) {
        this.purgeUserItems = purgeUserItems;
        this.mapper = mapper;
        this.initialBackoffMillis = initialBackoffMillis;
        this.retryBudget = retryBudget;
    }

    /** The process-wide count of records abandoned after their budget; read by
     *  {@link MetricsEndpoint}. */
    static long recordsDropped() {
        return RECORDS_DROPPED.get();
    }

    /**
     * READINESS, behind /health: true while the loop keeps COMPLETING cycles within the stall
     * tolerance. Cycles stop completing when a dependency is broken — database down, broker
     * unreachable, a record being retried inside its budget — so /health turning 503 means "this
     * instance cannot currently do its saga share", whether or not the thread itself is fine. "Broker
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
     *
     * <p>The retrying of a RECORD is bounded by {@link #RETRY_BUDGET} — a record still failing when
     * its deadline passes is committed and abandoned (see {@link #dropAfterBudget}), because a purge
     * that lands after the saga compensated erases the content of an account the leaver was told he
     * kept. Failures with no record behind them — the silent broker above, a poll or the batch's
     * {@code commitSync()} — keep retrying without end: nothing was consumed, so no record's clock is
     * running and there is nothing to lose by waiting.
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
        RecordBudget recordBudget = new RecordBudget(retryBudget);
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
                    try {
                        handleRecord(record, producer);
                    } catch (InterruptException | InterruptedException stopping) {
                        throw stopping;   // a stop request is not a handling failure
                    } catch (Exception handlingFailed) {
                        if (!recordBudget.isSpentOn(record)) {
                            // still inside this record's budget: fail the cycle so the batch is
                            // rewound and redelivered, exactly as before
                            throw handlingFailed;
                        }
                        dropAfterBudget(record, handlingFailed, consumer);
                        recordBudget.forget();
                    }
                }
                consumer.commitSync();
                recordBudget.forget();   // the batch is done; no record is being retried any more
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

    /**
     * The deadline of the ONE record currently being retried. A record is identified by its
     * coordinates, so the deadline survives the rewind: the same offset coming back from the broker
     * continues the clock that its first failure started, instead of being handed a fresh budget on
     * every redelivery (which is how a bounded budget turns back into an unbounded one).
     *
     * <p>{@code System.nanoTime}, never the wall clock: this measures elapsed time, and a wall clock
     * can step (NTP). Backwards it would hand out a budget that never expires; forwards it would cut a
     * purge short in the middle of the outage the budget exists for.
     */
    private static final class RecordBudget {

        private final Duration budget;
        private String retrying;
        private long deadlineNanos;

        RecordBudget(Duration budget) {
            this.budget = budget;
        }

        /**
         * True once this record has been failing for longer than the budget. The FIRST failure only
         * opens the deadline (it returns false), so a record's life here is the budget plus that first
         * attempt — the same shape as comments' {@code SagaRetryBudget.start()}.
         */
        boolean isSpentOn(ConsumerRecord<?, ?> record) {
            String at = record.topic() + "-" + record.partition() + "@" + record.offset();
            if (!at.equals(retrying)) {
                retrying = at;
                deadlineNanos = System.nanoTime() + budget.toNanos();
                return false;
            }
            return System.nanoTime() - deadlineNanos >= 0;
        }

        /** No record is being retried any more: the next failure opens a fresh budget. */
        void forget() {
            retrying = null;
        }
    }

    /**
     * The budget is spent: abandon this ONE record — loudly, counted, and with its offset committed
     * straight away so a later rewind in the same batch cannot bring it back. The rest of the batch
     * still gets its chance, and the cycle can complete, which is what lets readiness recover.
     *
     * <p>Only exception TYPES are logged at ERROR, never messages: a store failure's message can
     * carry the statement, and the statement carries the leaver's address — the same PII rule
     * {@link #handle} follows. The type chain is what an operator triages on anyway ("connection
     * refused" versus "rolled back" is a class, not a sentence), the coordinates say which record, and
     * the throwable itself is one DEBUG line away.
     *
     * <p>If the targeted commit itself fails the exception leaves through the loop's generic catch —
     * a commit failure is one of the two cases that still retry without end — and the record will be
     * dropped again later, counting twice. Over-counting a drop is the harmless direction.
     */
    private void dropAfterBudget(ConsumerRecord<String, String> record, Exception failure,
                                 Consumer<String, String> consumer) {
        RECORDS_DROPPED.incrementAndGet();
        String cid = header(record, CID_HEADER);
        if (cid != null) {
            MDC.put("cid", cid);   // the drop line belongs to the trace of the deletion request
        }
        try {
            LOG.error("giving up on {}-{}@{} after the {}s retry budget: the record is DROPPED and"
                            + " its offset committed ({}). If this was a purge command, this"
                            + " service did NOT erase that account's collections and the saga will"
                            + " compensate — it must never land after that",
                    record.topic(), record.partition(), record.offset(),
                    retryBudget.toSeconds(), typeChain(failure));
            LOG.debug("the failure that exhausted the budget for {}-{}@{}", record.topic(),
                    record.partition(), record.offset(), failure);
        } finally {
            MDC.remove("cid");
        }
        consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
    }

    /** The exception types, outermost first — see {@link #dropAfterBudget} on why not the messages. */
    private static String typeChain(Throwable failure) {
        StringBuilder chain = new StringBuilder();
        Throwable current = failure;
        while (current != null && chain.length() < 200) {
            if (!chain.isEmpty()) {
                chain.append(" <- ");
            }
            chain.append(current.getClass().getSimpleName());
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;   // a self-referencing cause is not a loop here
        }
        return chain.toString();
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
                        new ProducerRecord<>(EVENTS_TOPIC, keyFor(confirmation.get()), confirmation.get());
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
     * The partition key of a confirmation — the SAGA, never the person.
     *
     * <p>This used to reuse the incoming command's key, and the orchestrator keys its commands by
     * the leaver's address, so every confirmation carried that address in plain sight on
     * {@code usercollections-events}: visible in any broker tool, in every consumer's log line about
     * a key, and retained for as long as the topic is. The sibling service refuses to do that and
     * says why in its own javadoc — this participant simply never got the same treatment (P18 poz. 38).
     *
     * <p>The saga id is the natural key: it names the case, it is stable across redeliveries (which
     * is the only property a key must have here, since a confirmation is idempotent and the router
     * reads every partition), and it carries no personal data. A command without one — tolerated
     * only for older producers — falls back to an id DERIVED from the address, the same
     * {@code nameUUIDFromBytes} idiom the orchestrator uses for its re-published outcomes: never
     * blank, and never the address itself.
     */
    private String keyFor(String confirmationPayload) {
        try {
            com.fasterxml.jackson.databind.JsonNode confirmation = mapper.readTree(confirmationPayload);
            String sagaId = confirmation.path("sagaId").asText(null);
            if (sagaId != null && !sagaId.isBlank()) {
                return sagaId;
            }
            String email = confirmation.path("email").asText("");
            return UUID.nameUUIDFromBytes(email.getBytes(StandardCharsets.UTF_8)).toString();
        } catch (Exception unparseable) {
            // we built this payload a moment ago, so this cannot happen — but a key must exist, and
            // a random one still beats putting the address on the wire
            return UUID.randomUUID().toString();
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
