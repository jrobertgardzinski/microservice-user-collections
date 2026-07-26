package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.ItemReferences;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The cascade loop itself, driven end to end on kafka-clients' own {@link MockConsumer} through
 * the package-private {@code run(Consumer)} seam — no broker, no new dependency, the real loop.
 *
 * <p>What this pins is the loop's POLICY, which is the deliberate opposite of the saga loop's next
 * door: both cascade topics feed one thread, offsets advance once a batch has been handled, a
 * transient store failure is retried a bounded number of times, and a permanent one is abandoned
 * with the dead rows left behind rather than wedging the partition. The last of those is the
 * design's whole thesis, so it gets the longest test.
 */
class CascadeConsumerLoopTest {

    private static final TopicPartition MEMES =
            new TopicPartition(CascadeConsumer.MEMES_TOPIC, 0);
    private static final TopicPartition COMMENTS =
            new TopicPartition(CascadeConsumer.COMMENTS_TOPIC, 0);

    private static final String MEME = "3a8f0f6e-1b2c-4d5e-8f90-1a2b3c4d5e6f";
    private static final String COMMENT_1 = "11111111-1111-4111-8111-111111111111";

    private static final long TEST_BACKOFF_MILLIS = 5;   // retries in millis, not the real second

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
    void one_loop_serves_both_cascade_topics_and_commits_what_it_handled() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, () -> {
            consumer.addRecord(record(MEMES, 0, MEME, memeDeleted(MEME)));
            consumer.addRecord(record(COMMENTS, 0, MEME, commentsDeleted(MEME, COMMENT_1)));
        });

        startLoop(cascadeOver(store), consumer);
        await("both partitions to commit",
                () -> committedOffset(consumer, MEMES) >= 1
                        && committedOffset(consumer, COMMENTS) >= 1);

        assertTrue(store.list("alice@example.com", "favourites").isEmpty(),
                "one thread cascaded both a meme and its comments");
    }

    @Test
    void the_correlation_id_rides_from_the_header_into_the_mdc() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        List<String> cidsSeenByTheUseCase = new CopyOnWriteArrayList<>();
        // read the MDC from INSIDE the purge — that is where a log line would read it, and the
        // loop clears it again on the way out
        ItemReferences watching = (itemType, itemIds) -> {
            cidsSeenByTheUseCase.add(String.valueOf(MDC.get("cid")));
            return store.purge(itemType, itemIds);
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, () -> consumer.addRecord(record(MEMES, 0, MEME, memeDeleted(MEME))));

        startLoop(cascadeOver(watching), consumer);
        await("the record to be handled", () -> !cidsSeenByTheUseCase.isEmpty());

        assertEquals("cid-42", cidsSeenByTheUseCase.get(0),
                "the cascade must continue the trace of the request that deleted the meme");
    }

    @Test
    void a_transient_store_failure_is_retried_and_the_cascade_still_happens() throws Exception {
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        AtomicInteger failuresLeft = new AtomicInteger(1);
        AtomicInteger attempts = new AtomicInteger();
        ItemReferences failingOnce = (itemType, itemIds) -> {
            attempts.incrementAndGet();
            if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("database away for a moment");
            }
            return store.purge(itemType, itemIds);
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, () -> consumer.addRecord(record(MEMES, 0, MEME, memeDeleted(MEME))));

        startLoop(cascadeOver(failingOnce), consumer);
        await("the retried record's offset to commit", () -> committedOffset(consumer, MEMES) >= 1);

        assertEquals(2, attempts.get(), "one failure, one successful retry — in place, no rewind");
        assertTrue(store.list("alice@example.com", "favourites").isEmpty(),
                "a database hiccup must not cost a cleanup");
    }

    @Test
    void a_permanently_broken_store_leaves_a_dead_row_instead_of_wedging_the_partition()
            throws Exception {
        // THE thesis of this whole consumer. The saga loop next door would retry this forever and
        // hold the partition, because a lost purge there is a broken GDPR promise. Here the event
        // is abandoned after a bounded number of attempts: the ref survives its meme (the UI
        // renders it as unavailable), and — this is the part worth the test — the NEXT deletion
        // still gets cascaded, which an endless retry would have made impossible.
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT_1));
        AtomicInteger memeAttempts = new AtomicInteger();
        ItemReferences brokenForMemesOnly = (itemType, itemIds) -> {
            if (CascadeConsumer.MEME_ITEM_TYPE.equals(itemType)) {
                memeAttempts.incrementAndGet();
                throw new IllegalStateException("database permanently away for this one");
            }
            return store.purge(itemType, itemIds);
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, () -> {
            consumer.addRecord(record(MEMES, 0, MEME, memeDeleted(MEME)));
            consumer.addRecord(record(COMMENTS, 0, MEME, commentsDeleted(MEME, COMMENT_1)));
        });

        startLoop(cascadeOver(brokenForMemesOnly), consumer);
        await("the batch to be given up on and committed",
                () -> committedOffset(consumer, MEMES) >= 1);

        assertEquals(CascadeConsumer.MAX_ATTEMPTS, memeAttempts.get(),
                "bounded: tried " + CascadeConsumer.MAX_ATTEMPTS + " times, then abandoned");
        assertEquals(List.of(new ItemRef("meme", MEME)),
                store.list("alice@example.com", "favourites"),
                "the dead row stays — and the comment ref, whose purge worked, is gone");
        assertTrue(loopThread.isAlive(), "and the loop lives on to cascade the next deletion");
    }

    @Test
    void a_poison_pill_is_committed_away_rather_than_retried() throws Exception {
        AtomicInteger purges = new AtomicInteger();
        ItemReferences counting = (itemType, itemIds) -> {
            purges.incrementAndGet();
            return store.purge(itemType, itemIds);
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        prime(consumer, () -> {
            consumer.addRecord(record(MEMES, 0, "k", "not json at all"));
            consumer.addRecord(record(MEMES, 1, "k", memeDeleted("not-a-uuid")));
            consumer.addRecord(record(MEMES, 2, "k", "{\"type\":\"MEME_UPLOADED\"}"));
        });

        startLoop(cascadeOver(counting), consumer);
        await("all three to be committed", () -> committedOffset(consumer, MEMES) >= 3);

        assertEquals(0, purges.get(),
                "nothing purgeable arrived — and nothing was retried either: no retry can fix a"
                        + " payload, so a poison pill must never be allowed to hold a partition");
    }

    @Test
    void an_interrupt_during_the_retry_backoff_ends_the_loop() throws Exception {
        // the backoff is the one place the loop sleeps; an InterruptedException swallowed there
        // would keep the thread alive past a shutdown request
        ItemReferences alwaysFailing = (itemType, itemIds) -> {
            throw new IllegalStateException("database permanently away");
        };
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicInteger attempts = new AtomicInteger();
        ItemReferences counting = (itemType, itemIds) -> {
            attempts.incrementAndGet();
            return alwaysFailing.purge(itemType, itemIds);
        };
        // a slow backoff, so the interrupt below lands inside one
        CascadeConsumer cascade = new CascadeConsumer(new PurgeDeletedItem(counting), mapper, 5_000);
        prime(consumer, () -> consumer.addRecord(record(MEMES, 0, MEME, memeDeleted(MEME))));

        startLoop(cascade, consumer);
        await("the loop to reach its first backoff", () -> attempts.get() >= 1);
        loopThread.interrupt();
        loopThread.join(2_000);

        assertFalse(loopThread.isAlive(), "an interrupt during the backoff must end the loop");
        assertEquals(-1, committedOffset(consumer, MEMES),
                "an interrupted batch must not commit");
    }

    // ---- the harness ----

    private CascadeConsumer cascadeOver(ItemReferences references) {
        return new CascadeConsumer(new PurgeDeletedItem(references), mapper, TEST_BACKOFF_MILLIS);
    }

    private void startLoop(CascadeConsumer cascade, MockConsumer<String, String> consumer) {
        loopThread = new Thread(() -> cascade.run(consumer), "cascade-loop-under-test");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    /** Beginning offsets for both partitions, then the assignment-and-records poll task. */
    private static void prime(MockConsumer<String, String> consumer, Runnable records) {
        Map<TopicPartition, Long> beginnings = new HashMap<>();
        beginnings.put(MEMES, 0L);
        beginnings.put(COMMENTS, 0L);
        consumer.updateBeginningOffsets(beginnings);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(MEMES, COMMENTS));   // subscribe() happened in the loop
            records.run();
        });
    }

    private static ConsumerRecord<String, String> record(TopicPartition partition, long offset,
                                                         String key, String payload) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                partition.topic(), partition.partition(), offset, key, payload);
        record.headers().add(CascadeConsumer.CID_HEADER, "cid-42".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static String memeDeleted(String memeId) {
        return "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + memeId + "\",\"eventId\":\"e-1\"}";
    }

    private static String commentsDeleted(String memeId, String... commentIds) {
        List<String> quoted = new ArrayList<>();
        for (String commentId : commentIds) {
            quoted.add('"' + commentId + '"');
        }
        return "{\"id\":\"00000000-0000-4000-8000-000000000001\",\"type\":\"COMMENTS_DELETED\","
                + "\"memeId\":\"" + memeId + "\",\"commentIds\":[" + String.join(",", quoted)
                + "],\"version\":1}";
    }

    /** The committed offset of one partition, or -1 while nothing has been committed yet. */
    private static long committedOffset(MockConsumer<String, String> consumer,
                                        TopicPartition partition) {
        OffsetAndMetadata committed = consumer.committed(Set.of(partition)).get(partition);
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
}
