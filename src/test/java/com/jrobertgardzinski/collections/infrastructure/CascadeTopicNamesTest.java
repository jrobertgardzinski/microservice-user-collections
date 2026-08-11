package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CONSUMER end of the cascade's topic names, and the reason this file exists at all.
 *
 * <p>The audit of 26.07 named it the system's most dangerous structural finding: nothing anywhere
 * asserted the NAME of a topic. Rename it on one side only — a typo, a "-v2" suffix, a copied
 * constant — and the producer publishes into the void, the consumer waits forever, no data is
 * deleted, no error is raised and CI is completely green. Every other signal the portal has
 * (a green suite, {@code docker compose ps}, {@code up=1} in Prometheus) survives that mistake
 * untouched, because every process involved is perfectly healthy.
 *
 * <p>So the name is pinned to a LITERAL here, and to the same literal on the producer side. The two
 * repositories cannot share a constant — they are separate git repositories, deployed and versioned
 * apart, and a shared library for two strings would couple their release cycles for nothing — so the
 * literal is duplicated deliberately, and each side says where its twin is:
 *
 * <ul>
 *   <li>{@code memes-events} is also pinned in <b>microservice-memes</b>,
 *       {@code memes-infrastructure/src/test/java/com/jrobertgardzinski/memes/infrastructure/MemeDeletedTopicTest.java}
 *       (producer: {@code KafkaMemeEvents#TOPIC});</li>
 *   <li>{@code comments-events} is also pinned in <b>microservice-comments</b>,
 *       {@code src/test/java/com/jrobertgardzinski/comments/infrastructure/CascadeTopicNamesTest.java}
 *       (producer: {@code KafkaCommentEvents#TOPIC}).</li>
 * </ul>
 *
 * <p><b>Changing either string here REQUIRES the matching change in the other repository, in the
 * file named above.</b> That is the price of separate repositories, and a failing test on both sides
 * is exactly what makes the price visible instead of silent.
 *
 * <p>The pacts carry the same names as message metadata, so the check also runs ACROSS the two
 * repositories rather than only inside each — see {@link MemeDeletedPactTest} and
 * {@link CommentsDeletedPactTest}.
 */
@Epic("Infrastructure")
@Feature("Deletion cascade")
@Story("Topic names")
class CascadeTopicNamesTest {

    /** See the class comment: the twin literal lives in microservice-memes. */
    private static final String MEMES_EVENTS = "memes-events";

    /** See the class comment: the twin literal lives in microservice-comments. */
    private static final String COMMENTS_EVENTS = "comments-events";

    private static final String MEME = "3a8f0f6e-1b2c-4d5e-8f90-1a2b3c4d5e6f";
    private static final String COMMENT = "11111111-1111-4111-8111-111111111111";

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final CascadeConsumer cascade =
            new CascadeConsumer(new PurgeDeletedItem(store), new ObjectMapper());

    @Test
    @DisplayName("the cascade's topic constants are the names the producers publish on")
    void the_constants_are_the_agreed_names() {
        assertEquals(MEMES_EVENTS, CascadeConsumer.MEMES_TOPIC,
                "microservice-memes publishes MEME_DELETED here — see the class comment before"
                        + " changing this");
        assertEquals(COMMENTS_EVENTS, CascadeConsumer.COMMENTS_TOPIC,
                "microservice-comments publishes COMMENTS_DELETED here — see the class comment"
                        + " before changing this");
    }

    @Test
    @DisplayName("the running listener subscribes to exactly those two topics — nothing else")
    void the_subscription_is_the_agreed_names() {
        // the constants above are only half the claim: what matters operationally is the
        // subscription the REAL loop opens, so it is read back off the consumer the loop drove
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // one poll, then out: the loop checks the interrupt flag at the top of every cycle
        consumer.schedulePollTask(() -> Thread.currentThread().interrupt());
        try {
            cascade.run(consumer);
        } finally {
            Thread.interrupted();   // clear the flag we set, whatever happened
        }

        assertEquals(Set.of(MEMES_EVENTS, COMMENTS_EVENTS), consumer.subscription(),
                "a subscription that drifts from the producers' topics deletes nothing and says"
                        + " nothing — the failure mode this whole class exists for");
    }

    @Test
    @DisplayName("a cascade event that arrives on any other topic is not acted on")
    void the_topic_is_half_of_each_event_contract() {
        // the mirror of the subscription check, from the handler's side: the type alone is never
        // enough, so a producer that keeps the type and moves the topic changes nothing here
        store.add("alice@example.com", "favourites", new ItemRef("meme", MEME));
        store.add("alice@example.com", "favourites", new ItemRef("comment", COMMENT));

        assertEquals(0, cascade.handle("memes-events-v2",
                "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + MEME + "\",\"eventId\":\"e-1\"}"));
        assertEquals(0, cascade.handle("comments-events-v2",
                "{\"id\":\"00000000-0000-4000-8000-000000000001\","
                        + "\"type\":\"COMMENTS_DELETED\",\"memeId\":\"" + MEME
                        + "\",\"commentIds\":[\"" + COMMENT + "\"],\"version\":1}"));

        assertTrue(List.of(new ItemRef("meme", MEME), new ItemRef("comment", COMMENT))
                        .containsAll(store.list("alice@example.com", "favourites")),
                "nothing may be purged on the strength of a type alone");
        assertEquals(2, store.list("alice@example.com", "favourites").size());
    }
}
