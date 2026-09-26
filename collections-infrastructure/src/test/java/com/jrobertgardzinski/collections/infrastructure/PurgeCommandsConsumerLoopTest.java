package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.InMemoryCollectionRepository;
import com.jrobertgardzinski.collections.closure.CollectionsClosureParticipant;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionRepository;
import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.SavedItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The run() loop itself — polled, handled, confirmed, committed — driven end to end on
 * kafka-clients' own {@link MockConsumer}/{@link MockProducer} (no broker, no new dependency)
 * through the package-private seam {@code run(Consumer, Producer)}. The mocks fake the transport
 * only: the code under test is the real loop, including its rewind, backoff and interrupt exits.
 *
 * <p>One MockConsumer quirk shapes the retry test: poll() clears the records it holds, so a batch
 * that the loop rewinds to is re-added via {@code schedulePollTask} — that re-add plays the role
 * of the broker redelivering from the committed offset.
 */
@Epic("Saga")
@Feature("Purge commands")
@Story("Consumer loop")
class PurgeCommandsConsumerLoopTest {

    private static final TopicPartition PARTITION =
            new TopicPartition(PurgeCommandsConsumer.COMMANDS_TOPIC, 0);
    private static final String PURGE_ALICE =
            "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"alice@example.com\",\"sagaId\":\"s-1\"}";
    private static final String PURGE_BOB =
            "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"bob@example.com\",\"sagaId\":\"s-2\"}";
    private static final long TEST_BACKOFF_MILLIS = 5;   // the seam's third argument: retries in
                                                         // milliseconds, not the production second

    private final ObjectMapper mapper = new ObjectMapper();
    // one per test, which is the point of it no longer being static: a counter shared between
    // tests had to be read "before" and compared relative, and a forgotten reset made a
    // green test out of a wrong number
    private final ExportedObservations observations = new ExportedObservations();
    private final InMemoryCollectionRepository store = new InMemoryCollectionRepository();

    private final ListAppender<ILoggingEvent> logLines = new ListAppender<>();

    private Thread loopThread;

    /** The drop of an exhausted record is only worth anything if it is LOUD, so the log is read. */
    @BeforeEach
    void tapTheLog() {
        logLines.start();
        consumerLoggers().forEach(logger -> logger.addAppender(logLines));
    }

    @AfterEach
    void untapTheLog() {
        consumerLoggers().forEach(logger -> logger.detachAppender(logLines));
    }

    /**
     * BOTH loggers, because the two halves of this path log for different reasons: the consumer
     * says what it could not read off the wire, and the participant
     * (collections_account-closure) says what it decided. A test watching only one of them goes
     * half blind the moment a line moves across that boundary — which is exactly what happened
     * when the participant was cut out of the consumer.
     */
    private static java.util.List<ch.qos.logback.classic.Logger> consumerLoggers() {
        return java.util.List.of(
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PurgeCommandsConsumer.class),
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CollectionsClosureParticipant.class));
    }

    @AfterEach
    void stopLoop() throws InterruptedException {
        if (loopThread != null) {
            loopThread.interrupt();
            loopThread.join(2_000);
            assertFalse(loopThread.isAlive(), "the loop must always die on interrupt");
        }
    }

    @Test
    void a_processed_record_is_confirmed_before_its_offset_commits() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        // capture, at the exact commit that covers the record, how many confirmations the
        // producer had already accepted — the at-least-once ordering made observable
        AtomicInteger confirmationsWhenOffsetCommitted = new AtomicInteger(-1);
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync() {
                if (assignment().contains(PARTITION) && position(PARTITION) == 1
                        && confirmationsWhenOffsetCommitted.get() < 0) {
                    confirmationsWhenOffsetCommitted.set(producer.history().size());
                }
                super.commitSync();
            }
        };
        prime(consumer, command(0, PURGE_ALICE));

        startLoop(consumerUnderTest(store), consumer, producer);
        await("the record's offset to commit", () -> committedOffset(consumer) >= 1);

        assertEquals(1, producer.history().size(), "exactly one confirmation");
        ProducerRecord<String, String> confirmation = producer.history().get(0);
        assertEquals(PurgeCommandsConsumer.EVENTS_TOPIC, confirmation.topic());
        JsonNode event = mapper.readTree(confirmation.value());
        assertEquals("USER_CONTENT_PURGED", event.path("type").asText());
        assertEquals("s-1", event.path("sagaId").asText());
        Header cid = confirmation.headers().lastHeader(PurgeCommandsConsumer.CID_HEADER);
        assertNotNull(cid, "the correlation id must ride out on the confirmation");
        assertEquals("cid-42", new String(cid.value(), StandardCharsets.UTF_8));
        assertTrue(store.list("alice@example.com", "favourites").isEmpty(), "purged");
        assertEquals(1, confirmationsWhenOffsetCommitted.get(),
                "the confirmation must reach the broker BEFORE the offset commits");
    }

    /**
     * The confirmation's partition key names the SAGA, not the person (P18 poz. 38).
     *
     * <p>It used to be the incoming command's key, and the orchestrator keys commands by the
     * leaver's address — so the address of the person being erased sat in plain sight on
     * usercollections-events, for the topic's whole retention. The sibling service refuses to do
     * that and explains why in its own javadoc; this participant just never got the same treatment.
     */
    @Test
    void the_confirmation_is_keyed_by_the_saga_never_by_the_leavers_address() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // the incoming command IS keyed by the address (that is what the orchestrator sends)
        prime(consumer, command(0, PURGE_ALICE));

        startLoop(consumerUnderTest(store), consumer, producer);
        await("the record's offset to commit", () -> committedOffset(consumer) >= 1);

        ProducerRecord<String, String> confirmation = producer.history().get(0);
        assertEquals("s-1", confirmation.key(),
                "the saga id is the key: it names the case, survives redelivery unchanged, and"
                        + " carries no personal data");
        assertFalse(confirmation.key().contains("alice@example.com"),
                "the leaver's address must never be the key — a broker tool shows keys to anyone"
                        + " who can list the topic");
    }

    @Test
    void a_store_failure_inside_the_budget_commits_nothing_and_the_loop_retries_after_backoff()
            throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicLong committedWhenStoreFailed = new AtomicLong(Long.MIN_VALUE);
        AtomicInteger failuresLeft = new AtomicInteger(1);
        // the throwing decorator: the first purge finds the database away, every later one works
        ItemErasure failingOnce = new DelegatingErasure(store) {
            @Override
            public List<SavedItem> activeOf(String user) {
                if (failuresLeft.getAndDecrement() > 0) {
                    committedWhenStoreFailed.set(committedOffset(consumer));
                    // MockConsumer.poll() cleared the batch; re-add it so the loop's rewind to
                    // the committed offset has something to be redelivered
                    consumer.schedulePollTask(() -> consumer.addRecord(command(0, PURGE_ALICE)));
                    throw new IllegalStateException("database away");
                }
                return super.activeOf(user);
            }
        };
        prime(consumer, command(0, PURGE_ALICE));

        startLoop(consumerUnderTest(failingOnce), consumer, producer);
        await("the retried record's offset to commit", () -> committedOffset(consumer) >= 1);

        assertTrue(failuresLeft.get() <= 0, "the failure must actually have fired");
        assertEquals(-1, committedWhenStoreFailed.get(),
                "a failed batch must not have been committed");
        assertEquals(1, producer.history().size(), "one confirmation, after the retry");
        assertTrue(store.list("alice@example.com", "favourites").isEmpty(), "purged on retry");
        assertTrue(loopThread.isAlive(), "an infrastructure failure must not kill the loop");
        assertEquals(0, observations.recordsDropped(),
                "a hiccup healed well inside the retry budget must drop nothing: the budget only"
                        + " bounds the retrying, it must not shorten it");
    }

    @Test
    void an_interrupt_mid_send_ends_the_loop_instead_of_being_swallowed() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        // autoComplete=false: send().get() blocks forever — the exact spot where the raw
        // InterruptedException used to be eaten by the generic retry catch
        MockProducer<String, String> producer =
                new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, command(0, PURGE_ALICE));

        startLoop(consumerUnderTest(store), consumer, producer);
        await("the loop to block awaiting the confirmation ack",
                () -> producer.history().size() == 1);
        loopThread.interrupt();
        loopThread.join(2_000);

        assertFalse(loopThread.isAlive(),
                "an interrupt during send().get() must end the loop cleanly");
        assertEquals(-1, committedOffset(consumer), "an interrupted batch must not commit");
    }

    @Test
    void an_empty_email_is_dropped_without_confirmation_but_with_commit() throws Exception {
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, command(0,
                "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"\",\"sagaId\":\"s-9\"}"));

        startLoop(consumerUnderTest(store), consumer, producer);
        await("the dropped command's offset to commit", () -> committedOffset(consumer) >= 1);

        assertTrue(producer.history().isEmpty(),
                "no confirmation for a purge that never happened — but the offset commits,"
                        + " because no retry can ever fix an empty email");
    }

    @Test
    void a_permanently_failing_store_keeps_alive_green_while_health_stalls() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicInteger failures = new AtomicInteger();
        // the poison-pill shape: every retry finds the database just as broken as the last one.
        // The consumer under test keeps the PRODUCTION budget (90s), so this whole test plays out
        // while the record's deadline is still far away — the drop that follows it has its own test
        ItemErasure alwaysFailing = new DelegatingErasure(store) {
            @Override
            public List<SavedItem> activeOf(String user) {
                failures.incrementAndGet();
                consumer.schedulePollTask(() -> consumer.addRecord(command(0, PURGE_ALICE)));
                throw new IllegalStateException("database permanently away");
            }
        };
        prime(consumer, command(0, PURGE_ALICE));
        PurgeCommandsConsumer purge = consumerUnderTest(alwaysFailing);

        startLoop(purge, consumer, producer);
        await("a few failed retry cycles", () -> failures.get() >= 3);
        Thread.sleep(150);   // age the FROZEN cycle marker well past the tolerance asserted below

        assertTrue(loopThread.isAlive(), "the loop must survive a permanently failing store");
        assertTrue(purge.alive(Duration.ofSeconds(30)),
                "/alive stays green: the thread keeps scheduling iterations, refreshing its"
                        + " marker at the top of every failing/backoff pass");
        assertFalse(purge.healthy(Duration.ofMillis(50)),
                "/health reports the stall: not one cycle has completed since the loop started");
        assertEquals(-1, committedOffset(consumer),
                "the failing batch must not commit while the record's retry budget is unspent");
    }

    @Test
    void a_record_still_failing_when_its_budget_ends_is_dropped_loudly_committed_and_counted()
            throws Exception {
        // the flaw this pins: the loop used to rewind and retry a failing record for ever, so a
        // collections database that came back half an hour later purged the collections of an
        // account the saga had long since compensated and handed back to its owner — with no
        // signal to him or to an operator. The budget ends the retrying while the saga still cares.
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicInteger failures = new AtomicInteger();
        ItemErasure alwaysFailing = new DelegatingErasure(store) {
            @Override
            public List<SavedItem> activeOf(String user) {
                failures.incrementAndGet();
                consumer.schedulePollTask(() -> consumer.addRecord(command(0, PURGE_ALICE)));
                // the message carries the address on purpose: the drop line must not repeat it
                throw new IllegalStateException(
                        new java.sql.SQLException("purge of alice@example.com rolled back"));
            }
        };
        prime(consumer, command(0, PURGE_ALICE));
        // a budget in milliseconds instead of the production 90 seconds — the same code path
        PurgeCommandsConsumer purge =
                consumerUnderTest(alwaysFailing, TEST_BACKOFF_MILLIS, Duration.ofMillis(60));

        startLoop(purge, consumer, producer);
        await("the abandoned record's offset to commit", () -> committedOffset(consumer) >= 1);

        assertTrue(failures.get() >= 2,
                "the budget must buy real retries, not just the first attempt");
        assertEquals(1, observations.recordsDropped(),
                "the drop must be COUNTED — collections_kafka_records_dropped_total is the only"
                        + " signal an operator gets that a deletion was not finished here");
        assertTrue(producer.history().isEmpty(),
                "a purge that never happened must not be confirmed as done");
        assertTrue(loopThread.isAlive(), "the loop goes on to the next record");
        await("readiness to recover once the loop moves on", () -> purge.healthy(Duration.ofSeconds(1)));

        ILoggingEvent drop = logLines.list.stream()
                .filter(line -> line.getLevel() == Level.ERROR)
                .findFirst()
                .orElseThrow(() -> new AssertionError("the drop must be LOUD: an ERROR line"));
        String logged = drop.getFormattedMessage();
        assertTrue(logged.contains("DROPPED"), "the line must say what happened: " + logged);
        assertTrue(logged.contains("IllegalStateException <- SQLException"),
                "the line must carry the exception TYPE chain, which is what triages: " + logged);
        assertFalse(logged.contains("alice@example.com"),
                "and never an exception MESSAGE — it can carry the leaver's address: " + logged);
        assertTrue(new MetricsEndpoint(observations).body()
                        .contains("collections_kafka_records_dropped_total{topic=\""
                                + PurgeCommandsConsumer.COMMANDS_TOPIC + "\"} "),
                "/metrics must expose the counter, or nobody can alert on it");
    }

    @Test
    void a_second_failing_record_in_the_batch_cannot_reset_the_first_ones_budget() throws Exception {
        // the budget is a promise about ONE record ("the deadline survives the rewind"), and a
        // rewind replays the WHOLE batch. With the deadline kept on a single key, two records
        // failing in turn — alice, then bob, then alice — handed each failure a fresh budget,
        // so neither was ever abandoned and a purge could still land minutes after the
        // orchestrator had compensated and given the account back.
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        store.add("bob@example.com", "favourites", new ItemRef("meme", "99"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicInteger aliceAttempts = new AtomicInteger();
        ItemErasure flapping = new DelegatingErasure(store) {
            @Override
            public List<SavedItem> activeOf(String user) {
                boolean failing = user.startsWith("bob")
                        || aliceAttempts.getAndIncrement() % 2 == 0;   // alice fails every other try
                if (!failing) {
                    return super.activeOf(user);
                }
                // the rewind's redelivery, the MockConsumer way: the whole batch comes back
                consumer.schedulePollTask(() -> {
                    consumer.addRecord(command(0, PURGE_ALICE));
                    consumer.addRecord(command(1, PURGE_BOB));
                });
                throw new IllegalStateException("database away for " + user);
            }
        };
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));
            consumer.addRecord(command(0, PURGE_ALICE));
            consumer.addRecord(command(1, PURGE_BOB));
        });
        PurgeCommandsConsumer purge =
                consumerUnderTest(flapping, TEST_BACKOFF_MILLIS, Duration.ofMillis(60));

        startLoop(purge, consumer, producer);
        await("a record whose budget ran out to be abandoned, though the OTHER record in the"
                        + " batch kept failing in between", () -> committedOffset(consumer) >= 1);

        assertTrue(aliceAttempts.get() >= 2, "the alternation must really have happened");
        assertTrue(observations.recordsDropped() >= 1,
                "a record that has been failing for longer than the whole budget must be dropped"
                        + " — counting the failures of its neighbour as its own progress is how an"
                        + " unbounded retry comes back");
    }

    @Test
    void a_commit_that_keeps_failing_retries_without_end_and_drops_nothing() throws Exception {
        // the other half of the fix: the budget bounds the handling of a RECORD. A failure with no
        // consumed record behind it — a commit, a poll, the broker probe — keeps retrying for ever,
        // because there is no purge in flight whose lateness could hurt anybody.
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        AtomicInteger commits = new AtomicInteger();
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync() {
                commits.incrementAndGet();
                schedulePollTask(() -> addRecord(command(0, PURGE_ALICE)));
                throw new org.apache.kafka.common.errors.TimeoutException("commit went nowhere");
            }
        };
        prime(consumer, command(0, PURGE_ALICE));
        PurgeCommandsConsumer purge =
                consumerUnderTest(store, TEST_BACKOFF_MILLIS, Duration.ofMillis(20));

        startLoop(purge, consumer, producer);
        await("several failed commits, well past the tiny budget", () -> commits.get() >= 5);

        assertEquals(0, observations.recordsDropped(),
                "a commit failure must never drop a record, however long it lasts");
        assertTrue(loopThread.isAlive(), "and must not end the loop either");
    }

    @Test
    void a_dead_broker_on_a_quiet_topic_fails_the_probe_and_stalls_health_not_alive()
            throws Exception {
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        java.util.concurrent.atomic.AtomicBoolean brokerAnswers =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        AtomicInteger failedProbes = new AtomicInteger();
        // a broker that stops answering: polls still "work" (empty — exactly what a real dead
        // broker returns), only the round-trip probe can tell the difference
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<org.apache.kafka.common.PartitionInfo> partitionsFor(
                    String topic, Duration timeout) {
                if (!brokerAnswers.get()) {
                    failedProbes.incrementAndGet();
                    throw new org.apache.kafka.common.errors.TimeoutException(
                            "simulated dead broker: no metadata answer within " + timeout);
                }
                return super.partitionsFor(topic, timeout);
            }
        };
        // a QUIET topic: assignment, then nothing but empty polls — the shape under which the
        // old cycle counter kept "completing" against a dead broker and /health lied 200
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION)));
        PurgeCommandsConsumer purge = consumerUnderTest(store);

        startLoop(purge, consumer, producer);
        await("the probe cadence to kick in and keep failing", () -> failedProbes.get() >= 3);
        Thread.sleep(150);   // age the FROZEN cycle marker well past the tolerance below

        assertTrue(loopThread.isAlive(), "a failing probe must not kill the loop");
        assertTrue(purge.alive(Duration.ofSeconds(30)),
                "/alive stays green: the thread schedules fine, it is the broker that is gone");
        assertFalse(purge.healthy(Duration.ofMillis(50)),
                "/health must report the dead broker even though every poll came back empty");

        // the broker answers again: the probe passes, cycles complete, readiness recovers
        brokerAnswers.set(true);
        await("a completed cycle once the broker answers again",
                () -> purge.healthy(Duration.ofMillis(500)));
    }

    @Test
    void a_failed_probe_rewinds_nothing_and_keeps_the_liveness_beat() throws Exception {
        // the regression this pins: the probe's TimeoutException used to land in the generic
        // catch, which raised the rewind flag — so the NEXT iteration walked committed() over
        // the assignment, one wait of up to default.api.timeout.ms per partition, for a batch
        // that was never consumed. Nothing was polled here, so nothing may be sought back.
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        AtomicInteger failedProbes = new AtomicInteger();
        AtomicInteger seeks = new AtomicInteger();
        AtomicInteger committedLookups = new AtomicInteger();
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<org.apache.kafka.common.PartitionInfo> partitionsFor(
                    String topic, Duration timeout) {
                failedProbes.incrementAndGet();
                throw new org.apache.kafka.common.errors.TimeoutException("dead broker");
            }

            @Override
            public synchronized void seek(TopicPartition partition, long offset) {
                seeks.incrementAndGet();
                super.seek(partition, offset);
            }

            @Override
            public synchronized void seekToBeginning(java.util.Collection<TopicPartition> parts) {
                seeks.incrementAndGet();
                super.seekToBeginning(parts);
            }

            @Override
            public synchronized Map<TopicPartition, OffsetAndMetadata> committed(
                    Set<TopicPartition> partitions) {
                committedLookups.incrementAndGet();
                return super.committed(partitions);
            }
        };
        // an ASSIGNED partition, so a rewind would have something to seek on if it happened
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION)));
        PurgeCommandsConsumer purge = consumerUnderTest(store);

        startLoop(purge, consumer, producer);
        await("several failing probes", () -> failedProbes.get() >= 3);

        assertEquals(0, seeks.get(),
                "a failed probe consumed nothing, so the next iteration must not rewind");
        assertEquals(0, committedLookups.get(),
                "and must not spend a default.api.timeout.ms committed() lookup on it either");
        assertTrue(purge.alive(Duration.ofSeconds(30)),
                "the beat keeps beating: the thread is scheduling, the broker is what is gone");
        assertTrue(loopThread.isAlive(), "a silent broker must not kill the loop");
    }

    @Test
    void the_probe_is_paced_by_the_clock_not_by_the_cycle_count() throws Exception {
        // under load a cycle takes microseconds, so a "every N cycles" cadence fired this
        // metadata round trip several times a second — the javadoc's promise that the probe
        // only costs anything on a quiet topic was simply untrue
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        AtomicInteger probes = new AtomicInteger();
        AtomicInteger cycles = new AtomicInteger();
        java.util.Set<String> probedTopics = java.util.concurrent.ConcurrentHashMap.newKeySet();
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<org.apache.kafka.common.PartitionInfo> partitionsFor(
                    String topic, Duration timeout) {
                probedTopics.add(topic);
                probes.incrementAndGet();
                return super.partitionsFor(topic, timeout);
            }

            @Override
            public synchronized void commitSync() {
                cycles.incrementAndGet();
                super.commitSync();
            }
        };
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> consumer.rebalance(List.of(PARTITION)));

        startLoop(consumerUnderTest(store), consumer, producer);
        // MockConsumer.poll returns at once, so these are the "cycles take milliseconds" case
        await("a burst of completed cycles", () -> cycles.get() >= 100);

        assertEquals(1, probes.get(),
                "exactly one probe — the first iteration's — inside a cadence window of "
                        + PurgeCommandsConsumer.PROBE_EVERY + ", however many cycles ran");
        assertEquals(Set.of(PurgeCommandsConsumer.COMMANDS_TOPIC), probedTopics,
                "the probe must ask about the one topic we consume, not the whole cluster");
    }

    @Test
    void a_finished_thread_lets_the_alive_marker_stall() throws Exception {
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, command(0, PURGE_ALICE));
        PurgeCommandsConsumer purge = consumerUnderTest(store);

        startLoop(purge, consumer, producer);
        await("the record's offset to commit", () -> committedOffset(consumer) >= 1);
        loopThread.interrupt();
        loopThread.join(2_000);
        assertFalse(loopThread.isAlive(), "the loop must have ended");

        Thread.sleep(250);   // nothing refreshes the marker any more — let it age for real
        assertFalse(purge.alive(Duration.ofMillis(100)),
                "a thread that exited stops refreshing the scheduled marker, and /alive is"
                        + " exactly the probe that must notice");
    }

    @Test
    void a_batch_whose_records_keep_finishing_stays_ready_past_the_tolerance() throws Exception {
        // The finding this pins: the readiness marker moved once per BATCH, and a batch is up to
        // MAX_POLL_RECORDS commands each paying its own synchronous send().get(). A poll that is
        // succeeding normally therefore outlives any tolerance an operator can sensibly set, and
        // /health calls a loop doing exactly what it should a stalled one. No tolerance value
        // fixes a per-batch marker — only counting a finished RECORD as progress does
        Duration tolerance = Duration.ofMillis(500);
        int records = 10;
        long perRecordMillis = 100;   // 10 x 100ms = one healthy batch twice the tolerance long
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        AtomicReference<PurgeCommandsConsumer> loop = new AtomicReference<>();
        List<Boolean> readyWhileHandling = new CopyOnWriteArrayList<>();
        ItemErasure slowStore = new DelegatingErasure(store) {
            @Override
            public List<SavedItem> activeOf(String user) {
                // the work one command really pays — a database round trip and, after it, the
                // confirmation's ack — made long enough to see without waiting out a real one
                sleepFor(perRecordMillis);
                readyWhileHandling.add(loop.get().healthy(tolerance));
                return super.activeOf(user);
            }
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));
            for (long offset = 0; offset < records; offset++) {
                consumer.addRecord(command(offset, PURGE_ALICE));   // ONE poll, ten commands
            }
        });
        PurgeCommandsConsumer purge = consumerUnderTest(slowStore);
        loop.set(purge);

        long startedNanos = System.nanoTime();
        startLoop(purge, consumer, producer);
        await("the whole batch to commit", () -> committedOffset(consumer) >= records);
        Duration batchTook = Duration.ofNanos(System.nanoTime() - startedNanos);

        assertEquals(records, readyWhileHandling.size(), "every record must have been handled");
        assertTrue(batchTook.compareTo(tolerance) > 0,
                "the test is only worth anything if the batch really outlasts the tolerance:"
                        + " it took " + batchTook.toMillis() + "ms against " + tolerance.toMillis()
                        + "ms");
        assertFalse(readyWhileHandling.contains(Boolean.FALSE),
                "a batch whose records keep finishing is a loop making progress, and /health must"
                        + " stay green through all of it — readiness went red at record "
                        + (readyWhileHandling.indexOf(Boolean.FALSE) + 1) + " of " + records);
    }

    // ---- the harness ----

    /** The per-record work, slowed down; a stop request cuts it short like any other block. */
    private static void sleepFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        }
    }

    private PurgeCommandsConsumer consumerUnderTest(ItemErasure erasure) {
        return consumerUnderTest(erasure, TEST_BACKOFF_MILLIS, PurgeCommandsConsumer.RETRY_BUDGET);
    }

    /**
     * The three use cases the participant now has. The records these tests replay are MARK
     * commands, so the mark is the step a sabotaged store breaks — which is the point: the
     * reversible step is the one the orchestrator is waiting on, so it is the one whose failure
     * must survive a redelivery and end with the budget.
     */
    private PurgeCommandsConsumer consumerUnderTest(ItemErasure erasure, long backoffMillis,
                                                    Duration budget) {
        return new PurgeCommandsConsumer(new MarkUserItemsForErasure(erasure, Clock.systemUTC()),
                new RestoreUserItems(erasure), new PurgeUserItems(erasure), mapper, backoffMillis,
                budget, observations);
    }

    private void startLoop(PurgeCommandsConsumer purge, MockConsumer<String, String> consumer,
                           MockProducer<String, String> producer) {
        loopThread = new Thread(() -> purge.run(consumer, producer), "purge-loop-under-test");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    /** Beginning offsets plus the assignment-and-first-record poll task, the MockConsumer way. */
    private static void prime(MockConsumer<String, String> consumer,
                              ConsumerRecord<String, String> first) {
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(PARTITION));   // subscribe() happened in the loop already
            consumer.addRecord(first);
        });
    }

    private static ConsumerRecord<String, String> command(long offset, String payload) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                PurgeCommandsConsumer.COMMANDS_TOPIC, 0, offset, "alice@example.com", payload);
        record.headers().add(PurgeCommandsConsumer.CID_HEADER,
                "cid-42".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    /** The committed offset, or -1 while nothing has been committed yet. */
    private static long committedOffset(MockConsumer<String, String> consumer) {
        OffsetAndMetadata committed = consumer.committed(Set.of(PARTITION)).get(PARTITION);
        return committed == null ? -1 : committed.offset();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for " + what);
    }

    /** An ItemErasure that forwards everything; tests override the method they sabotage. */
    private static class DelegatingErasure implements ItemErasure {
        private final ItemErasure delegate;

        DelegatingErasure(ItemErasure delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<SavedItem> activeOf(String user) {
            return delegate.activeOf(user);
        }

        @Override
        public List<SavedItem> pendingOf(String user) {
            return delegate.pendingOf(user);
        }

        @Override
        public void store(SavedItem state) {
            delegate.store(state);
        }

        @Override
        public int eraseMarked(String user) {
            return delegate.eraseMarked(user);
        }

        @Override
        public List<SavedItem> pendingSince(java.time.Instant cutoff) {
            return delegate.pendingSince(cutoff);
        }
    }
}
