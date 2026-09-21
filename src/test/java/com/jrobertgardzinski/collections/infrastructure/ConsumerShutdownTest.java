package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stop nobody used to deliver. Both Kafka loops in this service are written around an
 * interrupt — the saga consumer's own javadoc calls it "the ONLY stop signal here" — and nothing
 * ever sent one: the two daemon virtual threads were simply abandoned when the JVM went down, so
 * {@code KafkaConsumer.close()} never ran and neither consumer group was left. The next start of
 * the service then waited out {@code session.timeout.ms} (~45s) before anyone consumed
 * {@code content-commands} again, which on a saga hop is a deployment's worth of silence in the
 * middle of an account deletion. The sibling orchestrator has registered the hook all along.
 *
 * <p>What a test can reach and what it cannot: the registration is real (the JVM hands the hook
 * back only if it was registered, which is what {@code removeShutdownHook} answers) and so is the
 * stop path — the hook's own Runnable, run here instead of by the JVM. The close of a REAL
 * {@code KafkaConsumer} is not: it lives in {@code run(String)}'s try-with-resources behind a
 * broker this machine does not have, so the loops here run on kafka-clients' MockConsumer through
 * the same package-private seam the loop tests use, and what is proven about the close is that
 * the loops RETURN, which is the thing that lets a try-with-resources close anything at all.
 */
@Epic("Infrastructure")
@Feature("Shutdown")
class ConsumerShutdownTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemoryCollectionStore store = new InMemoryCollectionStore();

    @Test
    void the_stop_hook_is_registered_and_ends_both_consumer_loops() {
        MockConsumer<String, String> purgeClient = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<String, String> cascadeClient =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        PurgeCommandsConsumer purge = new PurgeCommandsConsumer(
                new MarkUserItemsForErasure(store, Clock.systemUTC()), new RestoreUserItems(store),
                new PurgeUserItems(store), mapper, Observations.<Observation>silent());
        CascadeConsumer cascade = new CascadeConsumer(new PurgeDeletedItem(store), mapper);
        // the shape Main gives them: a loop inside a try-with-resources, on its own thread
        Thread purgeThread = loop("purge-consumer", purgeClient, () -> purge.run(purgeClient, producer));
        Thread cascadeThread = loop("cascade-consumer", cascadeClient, () -> cascade.run(cascadeClient));

        Thread hook = Main.registerStopHook(purgeThread, cascadeThread);

        assertTrue(Runtime.getRuntime().removeShutdownHook(hook),
                "the stop must be registered WITH THE JVM, or nothing ever delivers the interrupt"
                        + " both loops are written around");
        hook.run();   // exactly what the JVM would have run, on this thread instead of its own

        assertFalse(purgeThread.isAlive(), "the saga consumer must end on the stop");
        assertFalse(cascadeThread.isAlive(), "and so must the cascade consumer");
        assertTrue(purgeClient.closed(), "the saga consumer's client must be closed, which is what"
                + " leaves the group instead of making the next start wait out session.timeout.ms");
        assertTrue(cascadeClient.closed(), "and the cascade consumer's client too — it has a group"
                + " of its own to leave");
    }

    /** One consumer thread as Main starts it: the loop, then the client's close on the way out. */
    private static Thread loop(String name, MockConsumer<String, String> client, Runnable body) {
        Thread thread = new Thread(() -> {
            try (client) {
                body.run();
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}
