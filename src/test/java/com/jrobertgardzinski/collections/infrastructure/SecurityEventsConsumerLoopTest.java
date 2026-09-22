package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import com.jrobertgardzinski.collections.application.UserItemsRekey;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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
 * The rename loop itself, on kafka-clients' own {@link MockConsumer} through the package-private
 * {@code run(Consumer)} seam — no broker, no new dependency, the real loop.
 *
 * <p>Two claims, and they are the two the deletion cascade next door deliberately does not make.
 * The first is the topic NAME, pinned to a literal here and to the same literal in
 * microservice-security ({@code AccountDeletionOrchestrator#FACTS_TOPIC}): rename it on one side
 * only and this service consumes nothing, re-keys nothing, logs nothing and stays entirely green —
 * the failure mode {@link CascadeTopicNamesTest} exists for, one topic further on. The second is
 * that a rename is never given up on: {@link CascadeConsumer} abandons an event after a few
 * attempts because its worst case is a dead row, while here the worst case is the defect this loop
 * was written to close.
 */
@Epic("Infrastructure")
@Feature("Address changes")
@Story("Consumer loop")
class SecurityEventsConsumerLoopTest {

    /** See the class comment: the twin literal lives in microservice-security. */
    private static final String SECURITY_EVENTS = "security-events";

    private static final TopicPartition FACTS = new TopicPartition(SECURITY_EVENTS, 0);

    private static final long TEST_BACKOFF_MILLIS = 5;   // retries in millis, not the real second

    private final ObjectMapper mapper = new ObjectMapper();

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
    @DisplayName("the running loop subscribes to the topic security announces renames on")
    void the_subscription_is_the_agreed_name() {
        assertEquals(SECURITY_EVENTS, SecurityEventsConsumer.TOPIC,
                "microservice-security announces EMAIL_CHANGED here — see the class comment before"
                        + " changing this");
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // one poll, then out: the loop checks the interrupt flag at the top of every cycle
        consumer.schedulePollTask(() -> Thread.currentThread().interrupt());
        try {
            loopOver((oldEmail, newEmail) -> 0).run(consumer);
        } finally {
            Thread.interrupted();   // clear the flag we set, whatever happened
        }

        assertEquals(Set.of(SECURITY_EVENTS), consumer.subscription(),
                "a subscription that drifts from the producer's topic re-keys nothing and says"
                        + " nothing — every read for a moved member stays wrong, silently");
    }

    @Test
    @DisplayName("a rename is retried until it lands: the offset never moves past an unhandled one")
    void a_failing_rekey_is_rewound_and_retried_rather_than_given_up_on() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<String> moved = new CopyOnWriteArrayList<>();
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        UserItemsRekey failingTwice = (oldEmail, newEmail) -> {
            if (attempts.incrementAndGet() <= 2) {
                // MockConsumer.poll() cleared the batch; re-add it so the loop's rewind to the
                // committed offset has something to be redelivered (the shape the saga loop's
                // own failure test uses)
                consumer.schedulePollTask(() -> consumer.addRecord(record(0, emailChanged())));
                throw new IllegalStateException("database away for a moment");
            }
            moved.add(oldEmail + " -> " + newEmail);
            return 1;
        };
        consumer.updateBeginningOffsets(Map.of(FACTS, 0L));
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(FACTS));   // subscribe() happened in the loop
            consumer.addRecord(record(0, emailChanged()));
        });

        startLoop(loopOver(failingTwice), consumer);
        await("the rename to be committed", () -> committedOffset(consumer) >= 1);

        assertEquals(3, attempts.get(),
                "two failures and a third attempt that worked — the rewind is what brings the"
                        + " record back, because poll() had already walked past it in memory");
        assertEquals(List.of("alice@old.example.com -> alice@new.example.com"), moved,
                "and the rename really happened before the offset moved: a rename dropped is a"
                        + " member whose lists read empty for ever, and nothing repeats the fact");
    }

    // ---- the harness ----

    private SecurityEventsConsumer loopOver(UserItemsRekey rekey) {
        return new SecurityEventsConsumer(new RekeyUserItems(rekey), mapper, TEST_BACKOFF_MILLIS);
    }

    private void startLoop(SecurityEventsConsumer consumer, MockConsumer<String, String> client) {
        loopThread = new Thread(() -> consumer.run(client), "security-events-loop-under-test");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    private static ConsumerRecord<String, String> record(long offset, String payload) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                FACTS.topic(), FACTS.partition(), offset, "alice@new.example.com", payload);
        record.headers().add(SecurityEventsConsumer.CID_HEADER,
                "cid-42".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static String emailChanged() {
        return "{\"id\":\"00000000-0000-4000-8000-000000000001\",\"type\":\"EMAIL_CHANGED\","
                + "\"oldEmail\":\"alice@old.example.com\",\"email\":\"alice@new.example.com\","
                + "\"version\":1}";
    }

    private static long committedOffset(MockConsumer<String, String> consumer) {
        OffsetAndMetadata committed = consumer.committed(Set.of(FACTS)).get(FACTS);
        return committed == null ? -1 : committed.offset();
    }

    private static void await(String what, BooleanSupplier done) throws InterruptedException {
        for (int attempt = 0; attempt < 400; attempt++) {
            if (done.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for " + what);
    }
}
