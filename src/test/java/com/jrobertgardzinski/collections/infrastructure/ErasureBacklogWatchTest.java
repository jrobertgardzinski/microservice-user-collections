package com.jrobertgardzinski.collections.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.SavedItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The alarm on a lost closure command — the only thing the passage of time is allowed to buy in
 * this design.
 *
 * <p>The failure it watches for is silent by construction: the saga marked the leaver's refs, the
 * closure never arrived, and from then on nothing is broken, nothing throws, and a query simply
 * returns fewer rows for ever. The rows are hidden (which the leaver asked for) and not erased
 * (which the GDPR asked for), and without this watch nobody would ever learn the difference.
 */
class ErasureBacklogWatchTest {

    private static final String LEAVER = "leaver@example.com";
    private static final Instant MARKED_AT = Instant.parse("2026-08-08T10:00:00Z");

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final ListAppender<ILoggingEvent> logLines = new ListAppender<>();

    @BeforeEach
    void tapTheLog() {
        logLines.start();
        watchLogger().addAppender(logLines);
    }

    @AfterEach
    void untapTheLog() {
        watchLogger().detachAppender(logLines);
        logLines.stop();
    }

    private static Logger watchLogger() {
        return (Logger) LoggerFactory.getLogger(ErasureBacklogWatch.class);
    }

    private ErasureBacklogWatch watchAt(Instant now) {
        return new ErasureBacklogWatch(store, Clock.fixed(now, ZoneOffset.UTC),
                Duration.ofMinutes(30));
    }

    @Test
    @DisplayName("a saga still running is not an alarm — the marks are minutes old, not hours")
    void a_fresh_mark_is_not_a_backlog() {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));
        new MarkUserItemsForErasure(store, Clock.fixed(MARKED_AT, ZoneOffset.UTC)).execute(LEAVER);

        assertEquals(0, watchAt(MARKED_AT.plus(Duration.ofMinutes(5))).check());
        assertEquals(List.of(), logLines.list, "and it says nothing: an alarm that cries every"
                + " time a saga is merely in flight is an alarm operators mute");
    }

    @Test
    @DisplayName("a mark older than any saga can last is counted and said out loud")
    void an_overdue_mark_is_alarmed_on() {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));
        new MarkUserItemsForErasure(store, Clock.fixed(MARKED_AT, ZoneOffset.UTC)).execute(LEAVER);

        assertEquals(1, watchAt(MARKED_AT.plus(Duration.ofHours(2))).check());

        String said = logLines.list.stream().map(ILoggingEvent::getFormattedMessage)
                .reduce("", (a, b) -> a + "\n" + b);
        assertTrue(said.contains("hidden but NOT erased"),
                "the operator has to be told what state the data is in: " + said);
        assertTrue(said.contains("delete them on a timer"),
                "and that nothing will fix it by itself — that is the whole design: " + said);
        assertFalse(said.contains(LEAVER),
                "but never the address: the owners of these rows are exactly the people this"
                        + " service is trying to forget — " + said);
    }

    @Test
    @DisplayName("the closure landing clears the alarm by itself — that is why it is a gauge")
    void the_backlog_falls_back_to_zero() {
        store.add(LEAVER, "favourites", new ItemRef("meme", "42"));
        new MarkUserItemsForErasure(store, Clock.fixed(MARKED_AT, ZoneOffset.UTC)).execute(LEAVER);
        Instant later = MARKED_AT.plus(Duration.ofHours(2));
        assertEquals(1, watchAt(later).check());

        store.eraseMarked(LEAVER);   // the closure finally arrives

        assertEquals(0, watchAt(later).check(), "a counter would still read 1 here for ever");
    }

    @Test
    @DisplayName("a register that cannot be read keeps its last value instead of reporting zero")
    void an_unreadable_backlog_is_loud_not_reassuring() {
        ItemErasure unreadable = new ItemErasure() {
            public List<SavedItem> activeOf(String user) { return List.of(); }
            public List<SavedItem> pendingOf(String user) { return List.of(); }
            public void store(SavedItem state) { }
            public int eraseMarked(String user) { return 0; }

            public List<SavedItem> pendingSince(Instant cutoff) {
                throw new IllegalStateException("database away");
            }
        };

        int verdict = new ErasureBacklogWatch(unreadable, Clock.fixed(MARKED_AT, ZoneOffset.UTC),
                Duration.ofMinutes(30)).check();

        assertEquals(-1, verdict, "a failed read is not a clear backlog and must not read as one");
        assertTrue(logLines.list.stream().map(ILoggingEvent::getFormattedMessage)
                        .anyMatch(line -> line.contains("keeps its last value")),
                "and it says so: " + logLines.list);
    }
}
