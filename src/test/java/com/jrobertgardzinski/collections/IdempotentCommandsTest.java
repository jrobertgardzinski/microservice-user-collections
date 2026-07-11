package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
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
        c.put("purge the whole account",
                store -> new PurgeUserItems(store).execute("alice"));
        return c;
    }

    private static InMemoryCollectionStore seeded() {
        InMemoryCollectionStore store = new InMemoryCollectionStore();
        store.add("alice", "watchlist", COMMENT_7);
        store.add("bob", "favourites", MEME_42);   // a bystander no command may disturb
        return store;
    }

    /** The observable state, flattened — what "the same state" means in the law. */
    private static Map<String, List<ItemRef>> fingerprint(InMemoryCollectionStore store) {
        Map<String, List<ItemRef>> f = new LinkedHashMap<>();
        for (String user : new String[] {"alice", "bob"}) {
            for (String collection : new String[] {"favourites", "watchlist"}) {
                f.put(user + "/" + collection, store.list(user, collection));
            }
        }
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
