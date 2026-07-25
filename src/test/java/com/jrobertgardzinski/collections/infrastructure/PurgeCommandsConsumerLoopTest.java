package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.domain.ItemRef;
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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
class PurgeCommandsConsumerLoopTest {

    private static final TopicPartition PARTITION =
            new TopicPartition(PurgeCommandsConsumer.COMMANDS_TOPIC, 0);
    private static final String PURGE_ALICE =
            "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"alice@example.com\",\"sagaId\":\"s-1\"}";
    private static final long TEST_BACKOFF_MILLIS = 5;   // the seam's third argument: retries in
                                                         // milliseconds, not the production second

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryCollectionStore store = new InMemoryCollectionStore();

    private Thread loopThread;

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
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
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

    @Test
    void a_store_failure_commits_nothing_and_the_loop_retries_after_backoff() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicLong committedWhenStoreFailed = new AtomicLong(Long.MIN_VALUE);
        AtomicInteger failuresLeft = new AtomicInteger(1);
        // the throwing decorator: the first purge finds the database away, every later one works
        CollectionStore failingOnce = new DelegatingStore(store) {
            @Override
            public int purgeUser(String user) {
                if (failuresLeft.getAndDecrement() > 0) {
                    committedWhenStoreFailed.set(committedOffset(consumer));
                    // MockConsumer.poll() cleared the batch; re-add it so the loop's rewind to
                    // the committed offset has something to be redelivered
                    consumer.schedulePollTask(() -> consumer.addRecord(command(0, PURGE_ALICE)));
                    throw new IllegalStateException("database away");
                }
                return super.purgeUser(user);
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
    }

    @Test
    void an_interrupt_mid_send_ends_the_loop_instead_of_being_swallowed() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "42"));
        // autoComplete=false: send().get() blocks forever — the exact spot where the raw
        // InterruptedException used to be eaten by the generic retry catch
        MockProducer<String, String> producer =
                new MockProducer<>(false, new StringSerializer(), new StringSerializer());
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
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
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
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicInteger failures = new AtomicInteger();
        // the poison-pill shape: every retry finds the database just as broken as the last one
        CollectionStore alwaysFailing = new DelegatingStore(store) {
            @Override
            public int purgeUser(String user) {
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
        assertEquals(-1, committedOffset(consumer), "the failing batch must never commit");
    }

    @Test
    void a_finished_thread_lets_the_alive_marker_stall() throws Exception {
        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
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

    // ---- the harness ----

    private PurgeCommandsConsumer consumerUnderTest(CollectionStore backingStore) {
        return new PurgeCommandsConsumer(new PurgeUserItems(backingStore), mapper,
                TEST_BACKOFF_MILLIS);
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

    /** A CollectionStore that forwards everything; tests override the method they sabotage. */
    private static class DelegatingStore implements CollectionStore {
        private final CollectionStore delegate;

        DelegatingStore(CollectionStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean add(String user, String collection, ItemRef item) {
            return delegate.add(user, collection, item);
        }

        @Override
        public boolean remove(String user, String collection, ItemRef item) {
            return delegate.remove(user, collection, item);
        }

        @Override
        public List<ItemRef> list(String user, String collection) {
            return delegate.list(user, collection);
        }

        @Override
        public int purgeUser(String user) {
            return delegate.purgeUser(user);
        }
    }
}
