package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeDeletedItem;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.InMemoryCollectionStore;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The teeth of workspace ADR 0006: every command is idempotent BY DEFAULT — running it twice
 * must leave exactly the state running it once leaves. This one generic test enforces the law
 * for the whole service, so the feature file no longer needs a "twice is idempotent" scenario
 * per operation; the scenarios that remain there pin the REPLY contracts (ALREADY_SAVED,
 * NOT_SAVED), which are per-operation behaviour. A new command joins the law by joining
 * COMMANDS below.
 */
class IdempotentCommandsTest {

    /**
     * A frozen clock, so "twice equals once" is a statement about the STATE and not about two
     * different wall-clock readings. The rule that a redelivered mark keeps the FIRST instant is
     * pinned where it can be seen properly, on a real database, in JdbcCollectionStoreTest.
     */
    private static final java.time.Clock CLOCK = java.time.Clock.fixed(
            java.time.Instant.parse("2026-08-08T10:00:00Z"), java.time.ZoneOffset.UTC);

    private static final ItemRef MEME_42 = new ItemRef("meme", "42");
    private static final ItemRef COMMENT_7 = new ItemRef("comment", "7");

    /** Every command the service exposes, each exercised against a seeded store. */
    private static final Map<String, Consumer<InMemoryCollectionStore>> COMMANDS = commands();

    private static Map<String, Consumer<InMemoryCollectionStore>> commands() {
        Map<String, Consumer<InMemoryCollectionStore>> c = new LinkedHashMap<>();
        c.put("save into an empty collection",
                store -> new SaveItem(store).execute("alice", "favourites", MEME_42));
        c.put("save what is already there",
                store -> new SaveItem(store).execute("alice", "watchlist", COMMENT_7));
        c.put("remove what is there",
                store -> new RemoveItem(store).execute("alice", "watchlist", COMMENT_7));
        c.put("remove what is not there",
                store -> new RemoveItem(store).execute("alice", "favourites", COMMENT_7));
        c.put("mark the account for erasure",
                store -> new MarkUserItemsForErasure(store, CLOCK).execute("alice"));
        c.put("compensate a marked account",
                store -> {
                    new MarkUserItemsForErasure(store, CLOCK).execute("alice");
                    new RestoreUserItems(store).execute("alice");
                });
        c.put("close the saga on a marked account",
                store -> {
                    new MarkUserItemsForErasure(store, CLOCK).execute("alice");
                    new PurgeUserItems(store).execute("alice");
                });
        c.put("close a saga that marked nothing",
                store -> new PurgeUserItems(store).execute("alice"));
        c.put("compensate a saga that marked nothing",
                store -> new RestoreUserItems(store).execute("alice"));
        c.put("purge a deleted item everyone's collections may point at",
                store -> new PurgeDeletedItem(store).execute("comment", List.of("7")));
        c.put("purge a deleted item nobody saved",
                store -> new PurgeDeletedItem(store).execute("meme", List.of("does-not-exist")));
        return c;
    }

    private static InMemoryCollectionStore seeded() {
        InMemoryCollectionStore store = new InMemoryCollectionStore();
        store.add("alice", "watchlist", COMMENT_7);
        store.add("bob", "favourites", MEME_42);   // a bystander no command may disturb
        return store;
    }

    /**
     * The observable state, flattened — what "the same state" means in the law. The RESERVATIONS
     * are part of it, deliberately: a mark is invisible to {@link InMemoryCollectionStore#list} by
     * design, so a fingerprint built from listings alone would let every erasure command pass this
     * law without proving anything at all.
     */
    private static Map<String, Object> fingerprint(InMemoryCollectionStore store) {
        Map<String, Object> f = new LinkedHashMap<>();
        for (String user : new String[] {"alice", "bob"}) {
            for (String collection : new String[] {"favourites", "watchlist"}) {
                f.put(user + "/" + collection, store.list(user, collection));
            }
        }
        f.put("marks", store.marks());
        return f;
    }

    @TestFactory
    Stream<DynamicTest> every_command_twice_equals_once() {
        return COMMANDS.entrySet().stream().map(entry -> DynamicTest.dynamicTest(
                entry.getKey(), () -> {
                    InMemoryCollectionStore once = seeded();
                    entry.getValue().accept(once);

                    InMemoryCollectionStore twice = seeded();
                    entry.getValue().accept(twice);
                    entry.getValue().accept(twice);

                    assertEquals(fingerprint(once), fingerprint(twice),
                            "ADR 0006: a command run twice must leave the state of one run");
                }));
    }
}
