package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RekeyUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stop nobody used to deliver. Every Kafka loop in this service is written around an
 * interrupt — the saga consumer's own javadoc calls it "the ONLY stop signal here" — and nothing
 * ever sent one: the daemon virtual threads were simply abandoned when the JVM went down, so
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
    void the_stop_hook_is_registered_and_ends_every_consumer_loop() {
        MockConsumer<String, String> purgeClient = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<String, String> cascadeClient =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockConsumer<String, String> rekeyClient =
                new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        PurgeCommandsConsumer purge = new PurgeCommandsConsumer(
                new MarkUserItemsForErasure(store, Clock.systemUTC()), new RestoreUserItems(store),
                new PurgeUserItems(store), mapper, Observations.<Observation>silent());
        CascadeConsumer cascade = new CascadeConsumer(new PurgeDeletedItem(store), mapper);
        SecurityEventsConsumer rekey = new SecurityEventsConsumer(
                new RekeyUserItems((oldEmail, newEmail) -> 0), mapper);
        // the shape Main gives them: a loop inside a try-with-resources, on its own thread
        Thread purgeThread = loop("purge-consumer", purgeClient, () -> purge.run(purgeClient, producer));
        Thread cascadeThread = loop("cascade-consumer", cascadeClient, () -> cascade.run(cascadeClient));
        Thread rekeyThread =
                loop("security-events-consumer", rekeyClient, () -> rekey.run(rekeyClient));

        Thread hook = Main.registerStopHook(purgeThread, cascadeThread, rekeyThread);

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
        assertFalse(rekeyThread.isAlive(), "and the rename consumer, the third and newest loop —"
                + " the one a hook that took two named threads would have left running");
        assertTrue(rekeyClient.closed(), "with its own group left behind it as well");
    }

    @Test
    @DisplayName("every consumer thread the composition root starts is handed to the stop hook")
    void the_hook_is_given_every_loop_main_starts() throws IOException {
        // The test above drives the hook with three threads it built itself, so it can only prove
        // that the hook stops what it is given. What it cannot see is the call site — and the call
        // site is where a loop gets forgotten: the hook took two named threads for as long as there
        // were two, and the third would have been the one consumer nobody stops. That is a fact
        // about the SOURCE (main() binds a port and reads the environment, so no test drives it),
        // and this service already enforces one rule that way — see ItemReadFilterTest.
        String main = Files.readString(
                Path.of("src/main/java/com/jrobertgardzinski/collections/infrastructure/Main.java"));
        List<String> started = matches(Pattern.compile(
                "Thread\\s+(\\w+)\\s*=\\s*Thread\\.ofVirtual\\(\\)"), main);
        List<String> stopped = List.of(
                matches(Pattern.compile("registerStopHook\\(([^)]*)\\);"), main)
                        .get(0).split("\\s*,\\s*"));

        assertEquals(started, stopped,
                "these are the consumer threads main() starts and the ones it hands to the stop"
                        + " hook. A thread missing from the second list is a loop the JVM abandons"
                        + " mid-poll: its client is never closed, its group is never left, and the"
                        + " next start of this service waits out session.timeout.ms before anybody"
                        + " consumes that topic again");
    }

    private static List<String> matches(Pattern pattern, String source) {
        List<String> found = new ArrayList<>();
        Matcher matcher = pattern.matcher(source);
        while (matcher.find()) {
            found.add(matcher.group(1));
        }
        return found;
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
