package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import io.helidon.webserver.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Boots the Helidon 4 SE WebServer (virtual threads) and wires the use cases to their adapters.
 * The sixth flavour in the portfolio: imperative and blocking, yet scaling on Loom. Port comes
 * from {@code COLLECTIONS_PORT} (default 8092 — next free after the idp's 8091).
 *
 * <p>Storage is Postgres when {@code DB_URL} is set, else in-memory H2. Every collections route is
 * gated by microservice-security's JWKS ({@code SECURITY_URL}, default the local security).
 *
 * <p>Two probes, two questions. {@code /health} is READINESS: 503 once the consumer stops
 * completing cycles for longer than {@code COLLECTIONS_CONSUMER_STALL_SEC} (default 60) — a broken
 * dependency (database down, broker away, a poison pill in eternal retry) shows here, because a
 * cycle only completes when the whole poll-handle-confirm-commit chain works, and because at most
 * once per {@link PurgeCommandsConsumer#PROBE_EVERY} the loop demands a real answer from the
 * broker (empty polls return normally against a dead one, so a quiet topic alone proves nothing).
 * {@code /alive} is
 * LIVENESS: 503 only once the loop thread itself stops being scheduled for longer than
 * {@code COLLECTIONS_ALIVE_STALL_SEC} (default 120, floored at {@link #ALIVE_STALL_FLOOR} derived
 * from the producer's delivery timeout and the retry backoff) — its marker is refreshed at the top of every
 * iteration, failing and backoff ones included, so a database outage keeps /alive at 200 while
 * /health reports the stall. Restarting on /alive can heal a wedged process; restarting on /health
 * would just crash-loop against the broken dependency. Without {@code KAFKA_BOOTSTRAP_SERVERS} the
 * purge consumer never runs (dev, tests) — then neither probe has a loop to distrust and both stay
 * 200. Both stall envs are validated at startup: a zero or negative tolerance would declare every
 * start a stall, so it refuses to boot, naming the variable. The probes buy *visibility* (compose
 * marks the container unhealthy) — plain compose does not restart on an unhealthy probe; a restart
 * is an orchestrator's job (k3s, Swarm) acting on the same signal.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /** The safety margin the derived floor carries on top of the arithmetic: {@code alive()}
     *  compares the age of the marker with {@code <=}, and an iteration that legitimately
     *  spends every clock it is allowed still pays for a GC pause, a socket settling or the
     *  scheduler's own latency on top — an exactly-tight floor would call that a dead thread. */
    static final int FLOOR_MARGIN_PERCENT = 25;

    /** The /alive stall tolerance's floor, DERIVED from the consumer loop's own clocks (never a
     *  magic number — the same discipline as microservice-offboarding): during a broker outage
     *  one iteration legitimately holds a confirmation send for up to
     *  {@link PurgeCommandsConsumer#DELIVERY_TIMEOUT} (= {@link PurgeCommandsConsumer#MAX_BLOCK}),
     *  a commit or a rewind lookup for up to {@link PurgeCommandsConsumer#API_TIMEOUT}, and then
     *  backs off up to {@link PurgeCommandsConsumer#MAX_BACKOFF}, so the tolerance covers two of
     *  the longest of those plus a poll and a broker probe, plus {@link #FLOOR_MARGIN_PERCENT} —
     *  below that a mere broker outage could outlast the probe and read as a dead thread,
     *  restarting a pod a restart cannot fix. Currently 83s; the 120s default sits above. */
    static final Duration ALIVE_STALL_FLOOR = withMargin(
            max(max(PurgeCommandsConsumer.DELIVERY_TIMEOUT, PurgeCommandsConsumer.API_TIMEOUT),
                    PurgeCommandsConsumer.MAX_BACKOFF)
                    .multipliedBy(2)
                    .plus(PurgeCommandsConsumer.POLL_EVERY)
                    .plus(PurgeCommandsConsumer.PROBE_TIMEOUT));

    private static Duration max(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** The derived worst case plus {@link #FLOOR_MARGIN_PERCENT}, rounded UP to whole seconds —
     *  the tolerance is configured and logged in seconds, so the floor lives in them too. */
    private static Duration withMargin(Duration derived) {
        long millis = derived.toMillis() * (100 + FLOOR_MARGIN_PERCENT) / 100;
        return Duration.ofSeconds(Math.ceilDiv(millis, 1_000));
    }

    private Main() {
    }

    /**
     * The /alive tolerance floored at {@link #ALIVE_STALL_FLOOR}: a smaller configured value
     * would let an iteration legitimately blocked by a broker outage (send held up to the
     * delivery timeout, then the backoff) read as a wedged thread — the exact restart-loop the
     * readiness/liveness split exists to prevent. Floored loudly, through the logger, where the
     * service's own WARNs live. Package-private for the test.
     */
    static Duration flooredAliveStall(String name, Duration configured) {
        if (configured.compareTo(ALIVE_STALL_FLOOR) >= 0) {
            return configured;
        }
        LOG.warn("{}={}s is below the {}s floor derived from the loop's clocks (2 x max of"
                        + " delivery.timeout {}s, default.api.timeout {}s and max backoff {}s,"
                        + " plus the {}s poll and the {}s broker probe, plus {}% margin) — a"
                        + " broker outage legitimately holds an iteration that long, and a"
                        + " smaller tolerance would let /alive restart the service over a broker"
                        + " problem; using {}s instead",
                name, configured.toSeconds(), ALIVE_STALL_FLOOR.toSeconds(),
                PurgeCommandsConsumer.DELIVERY_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.API_TIMEOUT.toSeconds(),
                PurgeCommandsConsumer.MAX_BACKOFF.toSeconds(),
                PurgeCommandsConsumer.POLL_EVERY.toSeconds(),
                PurgeCommandsConsumer.PROBE_TIMEOUT.toSeconds(), FLOOR_MARGIN_PERCENT,
                ALIVE_STALL_FLOOR.toSeconds());
        return ALIVE_STALL_FLOOR;
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

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("COLLECTIONS_PORT", "8092"));
        String securityUrl = System.getenv().getOrDefault("SECURITY_URL", "http://localhost:8080");

        DataSource dataSource = Database.migratedDataSource();
        CollectionStore store = new JdbcCollectionStore(dataSource);
        SecurityGate gate = new JwtSecurityGate(securityUrl);

        CollectionsApi collections = new CollectionsApi(
                new SaveItem(store), new RemoveItem(store), new ListItems(store), gate);

        // the account-deletion saga's third participant: consume purge commands off Kafka when a
        // broker is configured (on a daemon virtual thread); without one, this simply never runs —
        // and then /health has no loop to distrust (dev, tests: always OK)
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "").trim();
        PurgeCommandsConsumer purgeConsumer = null;
        if (!bootstrap.isEmpty()) {
            PurgeCommandsConsumer consumer =
                    new PurgeCommandsConsumer(new PurgeUserItems(store), new ObjectMapper());
            Thread.ofVirtual().name("purge-consumer").start(() -> consumer.run(bootstrap));
            purgeConsumer = consumer;
        }
        PurgeCommandsConsumer watchedConsumer = purgeConsumer;
        Duration consumerStall = Duration.ofSeconds(stallSeconds("COLLECTIONS_CONSUMER_STALL_SEC",
                System.getenv().getOrDefault("COLLECTIONS_CONSUMER_STALL_SEC", "60")));
        // 120s default: covers the worst legitimate iteration gap during a broker outage — a
        // confirmation send blocked up to delivery.timeout (30s) plus one max backoff (30s) —
        // with room to spare; the floor is derived from those same constants (ALIVE_STALL_FLOOR)
        // so a smaller configured value cannot turn a broker outage into a fake dead-thread 503
        Duration aliveStall = flooredAliveStall("COLLECTIONS_ALIVE_STALL_SEC",
                Duration.ofSeconds(stallSeconds("COLLECTIONS_ALIVE_STALL_SEC",
                        System.getenv().getOrDefault("COLLECTIONS_ALIVE_STALL_SEC", "120"))));

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        // CORS first: a preflight is answered before anything else runs
                        .addFilter(CorsFilter.fromEnv(System.getenv("COLLECTIONS_ALLOWED_ORIGINS")))
                        .addFilter(new CorrelationFilter())
                        .get("/health", (req, res) -> {
                            // READINESS, not TCP-open: with Kafka configured this turns 503 once
                            // the purge-consumer loop stops completing cycles for longer than
                            // COLLECTIONS_CONSUMER_STALL_SEC — dependencies included (a DB
                            // outage or a poison pill stalls the cycles even though the thread
                            // lives). The compose healthcheck then shows the container as
                            // unhealthy (visibility; a restart would come from an orchestrator
                            // acting on the same probe)
                            if (watchedConsumer == null || watchedConsumer.healthy(consumerStall)) {
                                res.send("OK");
                            } else {
                                res.status(503).send("purge consumer stalled");
                            }
                        })
                        .get("/alive", (req, res) -> {
                            // LIVENESS: 200 as long as the loop thread keeps being scheduled —
                            // its marker moves on every iteration, failed and backoff ones too,
                            // so a broken dependency does NOT trip this probe; only a thread
                            // that exited or wedged past COLLECTIONS_ALIVE_STALL_SEC does. The
                            // probe an orchestrator may restart on — /health it should only
                            // route (or alert) on
                            if (watchedConsumer == null || watchedConsumer.alive(aliveStall)) {
                                res.send("OK");
                            } else {
                                res.status(503).send("purge consumer thread stalled");
                            }
                        })
                        .get("/metrics", MetricsEndpoint::handle)
                        .register("/collections", collections))
                .build()
                .start();

        System.out.println("user-collections listening on port " + server.port());
    }
}
