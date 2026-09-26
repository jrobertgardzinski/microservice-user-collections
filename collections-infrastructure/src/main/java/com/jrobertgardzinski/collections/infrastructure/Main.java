package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import io.helidon.webserver.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;

/**
 * Boots the Helidon 4 SE WebServer (virtual threads) and wires the use cases to their adapters.
 * The sixth flavour in the portfolio: imperative and blocking, yet scaling on Loom. Port comes
 * from {@code COLLECTIONS_PORT} (default 8092 — next free after the idp's 8091).
 *
 * <p>Storage is Postgres when {@code DB_URL} is set, else in-memory H2. Every collections route is
 * gated by microservice-security's JWKS ({@code SECURITY_URL}, default the local security).
 *
 * <p>Two probes, two questions. {@code /health} is READINESS: 503 once the consumer stops making
 * progress for longer than {@code COLLECTIONS_CONSUMER_STALL_SEC} (default 150, floored at
 * {@link #CONSUMER_STALL_FLOOR} — below it the service REFUSES to boot) — a broken
 * dependency (database down, broker away, a record retried inside its budget) shows here, because a
 * record only finishes when the whole poll-handle-confirm chain works, and because at most
 * once per {@link PurgeCommandsConsumer#PROBE_EVERY} the loop demands a real answer from the
 * broker (empty polls return normally against a dead one, so a quiet topic alone proves nothing).
 * {@code /alive} is
 * LIVENESS: 503 only once the loop thread itself stops being scheduled for longer than
 * {@code COLLECTIONS_ALIVE_STALL_SEC} (default 240, floored at {@link #ALIVE_STALL_FLOOR}, the SUM
 * of every block one iteration can spend — the rewind lookup, the broker probe, the poll, the
 * DATABASE, the confirmation send, the commit and the retry backoff, plus a
 * margin) — its marker is refreshed at the top of every
 * iteration, failing and backoff ones included, so a database outage keeps /alive at 200 while
 * /health reports the stall. Restarting on /alive can heal a wedged process; restarting on /health
 * would just crash-loop against the broken dependency. Without {@code KAFKA_BOOTSTRAP_SERVERS} the
 * purge consumer never runs (dev, tests) — then neither probe has a loop to distrust and both stay
 * 200. Both stall envs are validated at startup: a zero or negative tolerance would declare every
 * start a stall, so it refuses to boot, naming the variable. The probes buy *visibility* (compose
 * marks the container unhealthy) — plain compose does not restart on an unhealthy probe; a restart
 * is an orchestrator's job (k3s, Swarm) acting on the same signal.
 *
 * <p><b>Three Kafka threads, three different promises.</b> {@link PurgeCommandsConsumer} is the
 * saga participant described above — orchestrated, confirmed, retried within a bounded budget (see
 * {@link PurgeCommandsConsumer#RETRY_BUDGET}), watched by both probes.
 * {@link CascadeConsumer} is the deletion cascade — choreographed, unconfirmed, best-effort, in
 * its own consumer group and watched by NEITHER probe. The split is deliberate and argued in
 * {@link CascadeConsumer}'s javadoc; the short version is that a stalled cleanup must never be
 * able to report this instance as unable to do its saga share.
 * {@link SecurityEventsConsumer} is the address change — nobody waits for it either, but its
 * failure is not cleanup debt: rows left under an address a member no longer holds make every read
 * for that member wrong and their later erasure a claim about nothing, so it stands behind both
 * probes with the saga consumer. Its worst legal iteration is shorter than the one
 * {@link #WORST_ITERATION} sums, so the floors below cover it as they stand. Without
 * {@code KAFKA_BOOTSTRAP_SERVERS} none of the three runs (dev, tests) — and then the probes have no
 * loop to distrust and both stay 200.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /** The safety margin the derived floor carries on top of the arithmetic: {@code alive()}
     *  compares the age of the marker with {@code <=}, and an iteration that legitimately
     *  spends every clock it is allowed still pays for a GC pause, a socket settling or the
     *  scheduler's own latency on top — an exactly-tight floor would call that a dead thread. */
    static final int FLOOR_MARGIN_PERCENT = 25;

    /**
     * The worst LEGAL iteration of the purge loop, block by block — the sum of every clock one
     * cycle can spend, in the order {@link PurgeCommandsConsumer#run} spends them. Written out as
     * a sum on purpose: the floor used to be {@code 2 x max(...)} of the same clocks, a shape
     * that LOOKS conservative and is not. Two of the longest block (30s) plus a poll and a probe
     * came to 66s, while an honest worst iteration adds up to 146s — so the "safe minimum" sat
     * well below the case it was sold as covering. Every term below is one real block:
     *
     * <ul>
     *   <li>{@link PurgeCommandsConsumer#API_TIMEOUT} — the rewind's {@code committed()} lookup,
     *       when the previous cycle failed</li>
     *   <li>{@link PurgeCommandsConsumer#PROBE_TIMEOUT} — the /health honesty probe</li>
     *   <li>{@link PurgeCommandsConsumer#POLL_EVERY} — the poll itself</li>
     *   <li>{@link Database#WORST_BLOCK} — the purge's reads and writes; ONE such block, because
     *       the first database failure throws out of the cycle and the records behind it are
     *       never handled</li>
     *   <li>{@link PurgeCommandsConsumer#DELIVERY_TIMEOUT} — the confirmation's
     *       {@code send().get()} (= {@link PurgeCommandsConsumer#MAX_BLOCK} for a send still
     *       waiting on metadata); one, not one per record, for the same reason — a silent broker
     *       fails the FIRST send and the iteration ends there (which is why
     *       {@link PurgeCommandsConsumer#MAX_POLL_RECORDS} guards a different failure)</li>
     *   <li>{@link PurgeCommandsConsumer#API_TIMEOUT} again — {@code commitSync()}</li>
     *   <li>{@link PurgeCommandsConsumer#MAX_BACKOFF} — the pause before the retry, paid INSIDE
     *       the iteration that failed</li>
     * </ul>
     */
    static final Duration WORST_ITERATION = PurgeCommandsConsumer.API_TIMEOUT
            .plus(PurgeCommandsConsumer.PROBE_TIMEOUT)
            .plus(PurgeCommandsConsumer.POLL_EVERY)
            .plus(Database.WORST_BLOCK)
            .plus(PurgeCommandsConsumer.DELIVERY_TIMEOUT)
            .plus(PurgeCommandsConsumer.API_TIMEOUT)
            .plus(PurgeCommandsConsumer.MAX_BACKOFF);

    /** The /alive stall tolerance's floor, DERIVED from the loop's own clocks (never a magic
     *  number — the same discipline, and now the same arithmetic, as
     *  microservice-offboarding): {@link #WORST_ITERATION} plus {@link #FLOOR_MARGIN_PERCENT}.
     *  Below it a mere broker or database outage — every clock of which is legal and bounded —
     *  would read as a dead thread, restarting a pod a restart cannot fix. Currently 183s
     *  (146s + 25%); the 240s default sits above. */
    static final Duration ALIVE_STALL_FLOOR = withMargin(WORST_ITERATION);

    /** The code default for {@code COLLECTIONS_ALIVE_STALL_SEC}. A named constant, not a literal
     *  in {@code main()}, so the test can assert the one property a default must have: that it
     *  sits ABOVE {@link #ALIVE_STALL_FLOOR}. The previous 120s did not, once the floor was
     *  computed honestly — and a default that the floor silently corrects is a lie in the
     *  javadoc, the manifests and the operator's head at once. */
    static final Duration DEFAULT_ALIVE_STALL = Duration.ofSeconds(240);

    /**
     * The /health stall tolerance's floor, derived from the RECORD budget — because readiness
     * measures something else than liveness does. The loop deliberately keeps ONE record alive for
     * {@link PurgeCommandsConsumer#RETRY_BUDGET} (90s), retrying it across rewound cycles, and
     * while it does, nothing finishes: the readiness marker stands still by design. A tolerance
     * below that budget therefore reports a purge riding out a database restart as a stalled
     * consumer — the lamp turning red for precisely the self-healing case the budget exists to
     * cover, and an operator who learns to ignore the lamp has no lamp. The minute on top pays for
     * the attempts on either side of the budget: the failure that opens it and the one that
     * closes it are both outside the 90s. It does NOT stretch to two full {@link
     * Database#WORST_BLOCK}s, and is not meant to — unlike {@link #ALIVE_STALL_FLOOR}, which
     * must cover the worst legal iteration because a restart follows it, this is a MINIMUM
     * below which the lamp certainly lies. It is the allowance both Spring participants carry
     * ({@code SagaParticipantConfig.STALL_FLOOR}), so the three participants of one saga tell an
     * operator one story — the discipline {@link #ALIVE_STALL_FLOOR} already follows towards
     * microservice-offboarding.
     */
    static final Duration CONSUMER_STALL_FLOOR = PurgeCommandsConsumer.RETRY_BUDGET.plusSeconds(60);

    /** The code default for {@code COLLECTIONS_CONSUMER_STALL_SEC}: the floor itself, spelled as
     *  the constant rather than as 150, so the two can never drift apart. The old default was 60s
     *  — below the budget it was supposed to outlast, which is the whole finding. */
    static final Duration DEFAULT_CONSUMER_STALL = CONSUMER_STALL_FLOOR;

    /** The derived worst case plus {@link #FLOOR_MARGIN_PERCENT}, rounded UP to whole seconds —
     *  the tolerance is configured and logged in seconds, so the floor lives in them too. */
    private static Duration withMargin(Duration derived) {
        long millis = derived.toMillis() * (100 + FLOOR_MARGIN_PERCENT) / 100;
        return Duration.ofSeconds(Math.ceilDiv(millis, 1_000));
    }

    private Main() {
    }

    /**
     * The /alive tolerance floored at {@link #ALIVE_STALL_FLOOR}: one iteration can legally spend
     * EVERY block in {@link #WORST_ITERATION} back to back — they are alternatives only in the
     * happy case, and a broker outage arriving on top of a slow database pays them in sequence —
     * so a smaller configured value would let an outage read as a wedged thread, the exact
     * restart-loop the readiness/liveness split exists to prevent. Floored loudly, through the
     * logger, where the service's own WARNs live; the message spells the sum out term by term, so
     * an operator told "below the floor" can see WHICH clocks add up to it. Package-private for
     * the test.
     */
    static Duration flooredAliveStall(String name, Duration configured) {
        if (configured.compareTo(ALIVE_STALL_FLOOR) >= 0) {
            return configured;
        }
        LOG.warn("{}={}s is below the {}s floor, the SUM of one iteration's blocks (rewind"
                        + " lookup {}s + broker probe {}s + poll {}s + database {}s + send {}s"
                        + " + commit {}s + max backoff {}s = {}s, plus {}% margin) — an outage"
                        + " legitimately holds an iteration that long, and a smaller tolerance"
                        + " would let /alive restart the service over a broker or database"
                        + " problem; using {}s instead",
                name, configured.toSeconds(), ALIVE_STALL_FLOOR.toSeconds(),
                PurgeCommandsConsumer.API_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.PROBE_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.POLL_EVERY.toSeconds(),
                Database.WORST_BLOCK.toSeconds(),
                PurgeCommandsConsumer.DELIVERY_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.API_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.MAX_BACKOFF.toSeconds(),
                WORST_ITERATION.toSeconds(), FLOOR_MARGIN_PERCENT,
                ALIVE_STALL_FLOOR.toSeconds());
        return ALIVE_STALL_FLOOR;
    }

    /**
     * The /health tolerance, REFUSED rather than quietly raised when it sits below
     * {@link #CONSUMER_STALL_FLOOR} — the shape both Spring participants use for the same
     * decision, and the difference from {@link #flooredAliveStall} above is deliberate. A
     * liveness tolerance too small only ever costs a WARN and a correction nobody depends on
     * reading; a readiness tolerance is a number an operator writes into a healthcheck, a probe's
     * {@code failureThreshold} and a runbook, and correcting it behind their back leaves all
     * three quoting a tolerance the service does not use. So it says no, and says both numbers.
     * Package-private for the test.
     */
    static Duration requiredConsumerStall(String name, Duration configured) {
        if (configured.compareTo(CONSUMER_STALL_FLOOR) >= 0) {
            return configured;
        }
        throw new IllegalArgumentException(name + "=" + configured.toSeconds() + "s is below the "
                + CONSUMER_STALL_FLOOR.toSeconds() + "s floor: one purge command may legitimately"
                + " hold this loop for a whole " + PurgeCommandsConsumer.RETRY_BUDGET.toSeconds()
                + "s retry budget, plus the attempts that open and close it, and nothing finishes"
                + " while it does — so a smaller tolerance would report a purge riding out a"
                + " database restart as a stalled consumer. Raise it or leave it unset.");
    }

    /**
     * Parse a stall tolerance, failing FAST but READABLY: a bare NumberFormatException
     * ("For input string: ...") names neither the variable nor why the service died — this
     * message does. A zero or negative tolerance is refused too: it would declare every start
     * a stall and answer 503 forever, which is a configuration mistake, not a wish. Shared by
     * both stall envs (hence the name parameter). Package-private for the test.
     */
    static long stallSeconds(String name, String raw) {
        long seconds;
        try {
            seconds = Long.parseLong(raw.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(
                    name + " must be a whole number of seconds, got: \"" + raw + "\"", invalid);
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(
                    name + " must be a positive number of seconds (a tolerance of "
                            + seconds + " would report a stall from the first probe on), got: \""
                            + raw + "\"");
        }
        return seconds;
    }

    /** How often the erasure backlog is counted. A minute, like the siblings' scheduled watch. */
    private static final Duration BACKLOG_WATCH_INTERVAL = Duration.ofMinutes(1);

    /** How long the stop waits for one loop to notice the interrupt and close its client. The
     *  orchestrator's number: long enough for a poll and a leave-group round trip, short enough
     *  that a broker which is ALSO gone cannot hold the shutdown open (a container that will not
     *  die is killed anyway, and then the group is abandoned exactly as before). */
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

    /**
     * The JVM's shutdown hook over every Kafka loop, and the Thread it registered so a test can
     * prove the registration happened and then drive the stop itself. Each loop exits on an
     * interrupt and closes its consumer on the way out ({@code run(String)}'s
     * try-with-resources); joining is what makes that reachable at all, because daemon virtual
     * threads are otherwise simply abandoned when the last non-daemon thread goes. Varargs rather
     * than one parameter per loop, so that adding one — the rename consumer was the third — cannot
     * be forgotten by leaving a signature as it was.
     */
    static Thread registerStopHook(Thread... consumers) {
        Thread hook = new Thread(() -> stopConsumers(consumers), "collections-consumers-stop");
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    /**
     * Interrupt every loop FIRST and only then wait for them, so the two stops overlap instead of
     * queueing their timeouts one behind the other. Package-private for the test.
     */
    static void stopConsumers(Thread... consumers) {
        for (Thread consumer : consumers) {
            consumer.interrupt();
        }
        for (Thread consumer : consumers) {
            try {
                consumer.join(STOP_TIMEOUT.toMillis());
            } catch (InterruptedException stopping) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public static void main(String[] args) {
        ProfileGuard.requireDeclaredProfile("COLLECTIONS_PROFILE", System.getenv("COLLECTIONS_PROFILE"));
        int port = Integer.parseInt(System.getenv().getOrDefault("COLLECTIONS_PORT", "8092"));
        String securityUrl = System.getenv().getOrDefault("SECURITY_URL", "http://localhost:8080");

        DataSource dataSource = Database.migratedDataSource();
        // the concrete type, not the CollectionRepository port: this one adapter answers BOTH ports —
        // the user axis the API and the saga use, and the item axis the cascade uses (V2's index)
        JdbcCollectionRepository store = new JdbcCollectionRepository(dataSource);
        // the erasure-aware side of the same table, and the ONLY adapter here allowed to read a
        // row the account-deletion saga has reserved (ADR 0007)
        JdbcItemErasure erasure = new JdbcItemErasure(dataSource);
        // the same table along its THIRD axis: not a reference and not a reservation, but the one
        // column that says whose row this is — and the one that moves when a member's address does
        JdbcUserItemsRekey rekey = new JdbcUserItemsRekey(dataSource);
        SecurityGate gate = new JwtSecurityGate(securityUrl);

        // the composition root's one watcher: everything that states a fact is handed THIS, and
        // /metrics reads it back out. Swap it for Observations.<Observation>silent() and the service runs
        // unobserved rather than broken — which is the whole point of the port
        ExportedObservations observations = new ExportedObservations();

        CollectionsApi collections = new CollectionsApi(
                new SaveItem(store), new RemoveItem(store), new ListItems(store), gate);

        // the account-deletion saga's third participant: consume purge commands off Kafka when a
        // broker is configured (on a daemon virtual thread); without one, this simply never runs —
        // and then /health has no loop to distrust (dev, tests: always OK)
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "").trim();
        PurgeCommandsConsumer purgeConsumer = null;
        SecurityEventsConsumer rekeyConsumer = null;
        if (!bootstrap.isEmpty()) {
            PurgeCommandsConsumer consumer = new PurgeCommandsConsumer(
                    new MarkUserItemsForErasure(erasure, Clock.systemUTC()),
                    new RestoreUserItems(erasure), new PurgeUserItems(erasure), new ObjectMapper(),
                    observations);
            Thread purgeThread =
                    Thread.ofVirtual().name("purge-consumer").start(() -> consumer.run(bootstrap));
            purgeConsumer = consumer;

            // the erasure backlog alarm: rows a lost closure command left hidden but not erased.
            // It runs beside the saga consumer and only when there IS a broker — without one there
            // is no saga, so there are no marks and nothing to watch (the same coupling the two
            // Spring participants get for free from @EnableScheduling)
            ErasureBacklogWatch backlogWatch = new ErasureBacklogWatch(
                    new com.jrobertgardzinski.collections.application.WatchErasureBacklog(
                            erasure, com.jrobertgardzinski.collections.config.ErasureTolerance.DEFAULT,
                            observations, Clock.systemUTC()));
            Thread.ofVirtual().name("erasure-backlog-watch").start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    backlogWatch.check();
                    try {
                        Thread.sleep(BACKLOG_WATCH_INTERVAL);
                    } catch (InterruptedException stopping) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            // the deletion CASCADE, on a thread and a consumer group of its OWN (see
            // CascadeConsumer's javadoc for the full argument): choreographed, best-effort, no
            // confirmation and no compensation. Deliberately absent from the two probes below —
            // a stalled cleanup is a cleanup debt, not an instance that cannot serve, and a
            // /health that reddens for cleanup debt is a /health operators learn to ignore
            CascadeConsumer cascade =
                    new CascadeConsumer(new PurgeDeletedItem(store), new ObjectMapper());
            Thread cascadeThread = Thread.ofVirtual().name("cascade-consumer")
                    .start(() -> cascade.run(bootstrap));

            // the THIRD loop: renames off security-events. Watched by both probes like the saga
            // consumer and unlike the cascade — a stalled cleanup is cleanup debt, while a stalled
            // rename has this instance answering one member's reads under a name they have left
            SecurityEventsConsumer rekeyEvents =
                    new SecurityEventsConsumer(new RekeyUserItems(rekey), new ObjectMapper());
            Thread rekeyThread = Thread.ofVirtual().name("security-events-consumer")
                    .start(() -> rekeyEvents.run(bootstrap));
            rekeyConsumer = rekeyEvents;

            // every loop here is written around an interrupt, and until recently nobody ever
            // delivered one: the JVM took the daemon threads down mid-poll, KafkaConsumer.close()
            // never ran and no group was left, so every restart of this service began with
            // ~session.timeout.ms of nobody consuming content-commands — a saga hop delayed for
            // the length of a deploy. The orchestrator has registered this hook all along
            registerStopHook(purgeThread, cascadeThread, rekeyThread);
        }
        PurgeCommandsConsumer watchedConsumer = purgeConsumer;
        SecurityEventsConsumer watchedRekey = rekeyConsumer;
        Duration consumerStall = requiredConsumerStall("COLLECTIONS_CONSUMER_STALL_SEC",
                Duration.ofSeconds(stallSeconds("COLLECTIONS_CONSUMER_STALL_SEC",
                        System.getenv().getOrDefault("COLLECTIONS_CONSUMER_STALL_SEC",
                                String.valueOf(DEFAULT_CONSUMER_STALL.toSeconds())))));
        // 240s default: it has to sit ABOVE ALIVE_STALL_FLOOR, and the floor is now the honest
        // SUM of one iteration's blocks (rewind lookup 20s + probe 5s + poll 1s + database 40s +
        // send 30s + commit 20s + backoff 30s = 146s, plus 25% margin = 183s) rather than the
        // 2 x max(...) shape that used to under-count it at 83s. The old 120s default was BELOW
        // that honest floor, so every boot would have been silently floored — a default that
        // needs correcting is not a default
        Duration aliveStall = flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC",
                Duration.ofSeconds(stallSeconds("COLLECTIONS_ALIVE_STALL_SEC",
                        System.getenv().getOrDefault("COLLECTIONS_ALIVE_STALL_SEC",
                                String.valueOf(DEFAULT_ALIVE_STALL.toSeconds())))));

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        // CORS first: a preflight is answered before anything else runs
                        .addFilter(CorsFilter.fromEnv(System.getenv("COLLECTIONS_ALLOWED_ORIGINS")))
                        .addFilter(new CorrelationFilter())
                        .get("/health", (req, res) -> {
                            // READINESS, not TCP-open: with Kafka configured this turns 503 once
                            // the purge-consumer loop stops finishing records for longer than
                            // COLLECTIONS_CONSUMER_STALL_SEC — dependencies included (a DB
                            // outage or a poison pill stalls the cycles even though the thread
                            // lives). The compose healthcheck then shows the container as
                            // unhealthy (visibility; a restart would come from an orchestrator
                            // acting on the same probe)
                            if (watchedConsumer != null && !watchedConsumer.healthy(consumerStall)) {
                                res.status(503).send("purge consumer stalled");
                            } else if (watchedRekey != null && !watchedRekey.healthy(consumerStall)) {
                                res.status(503).send("security events consumer stalled");
                            } else {
                                res.send("OK");
                            }
                        })
                        .get("/alive", (req, res) -> {
                            // LIVENESS: 200 as long as the loop thread keeps being scheduled —
                            // its marker moves on every iteration, failed and backoff ones too,
                            // so a broken dependency does NOT trip this probe; only a thread
                            // that exited or wedged past COLLECTIONS_ALIVE_STALL_SEC does. The
                            // probe an orchestrator may restart on — /health it should only
                            // route (or alert) on
                            if (watchedConsumer != null && !watchedConsumer.alive(aliveStall)) {
                                res.status(503).send("purge consumer thread stalled");
                            } else if (watchedRekey != null && !watchedRekey.alive(aliveStall)) {
                                res.status(503).send("security events consumer thread stalled");
                            } else {
                                res.send("OK");
                            }
                        })
                        .get("/metrics", new MetricsEndpoint(observations)::handle)
                        .register("/collections", collections))
                .build()
                .start();

        System.out.println("user-collections listening on port " + server.port());
    }
}
