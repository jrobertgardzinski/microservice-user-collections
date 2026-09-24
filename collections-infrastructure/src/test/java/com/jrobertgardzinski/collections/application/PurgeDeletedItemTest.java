package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.InMemoryCollectionStore;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cascade's use case, pinned as a set of claims rather than an implementation: it removes
 * EVERY user's reference to a deleted thing, it removes ONLY that thing's references, and running
 * it again is free. The blast radius is what the tests spend most of their words on — this is the
 * one operation in the service that reaches across users, so "it took something that was not
 * meant to go" is the failure that matters, not "it missed one".
 */
@Epic("Use case")
@Feature("Purge deleted item")
class PurgeDeletedItemTest {

    private InMemoryCollectionStore store;
    private PurgeDeletedItem purge;

    @BeforeEach
    void freshStore() {
        store = new InMemoryCollectionStore();
        purge = new PurgeDeletedItem(store);
    }

    @Test
    void it_removes_the_deleted_item_from_every_user_and_every_collection() {
        store.add("alice@example.com", "favourites", new ItemRef("meme", "m-1"));
        store.add("alice@example.com", "watchlist", new ItemRef("meme", "m-1"));
        store.add("bob@example.com", "favourites", new ItemRef("meme", "m-1"));

        assertEquals(3, purge.execute("meme", List.of("m-1")),
                "the count is what the log reports — all three refs went");

        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertTrue(store.list("alice@example.com", "watchlist").isEmpty());
        assertTrue(store.list("bob@example.com", "favourites").isEmpty(),
                "the cascade is not scoped to one user — that is its whole point");
    }

    @Test
    void it_takes_nobody_elses_refs_with_it() {
        // the neighbours a wrong WHERE clause would eat: same id under another type, same type
        // under another id, and a ref that merely happens to live beside the doomed one
        store.add("alice@example.com", "favourites", new ItemRef("meme", "shared-id"));
        store.add("alice@example.com", "favourites", new ItemRef("comment", "shared-id"));
        store.add("alice@example.com", "favourites", new ItemRef("meme", "another"));
        store.add("bob@example.com", "watchlist", new ItemRef("comment", "shared-id"));

        assertEquals(1, purge.execute("meme", List.of("shared-id")));

        assertEquals(List.of(new ItemRef("comment", "shared-id"), new ItemRef("meme", "another")),
                sorted(store.list("alice@example.com", "favourites")),
                "only the (meme, shared-id) ref may go: the type is half the key");
        assertEquals(List.of(new ItemRef("comment", "shared-id")),
                store.list("bob@example.com", "watchlist"));
    }

    @Test
    void running_it_again_removes_nothing_and_raises_nothing() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", "c-1"));

        assertEquals(1, purge.execute("comment", List.of("c-1")));
        assertEquals(0, purge.execute("comment", List.of("c-1")),
                "at-least-once delivery needs no dedup: the second run is a no-op, not an error");
        assertEquals(0, purge.execute("comment", List.of("c-1")));
    }

    @Test
    void an_item_nobody_saved_is_zero_not_a_failure() {
        assertEquals(0, purge.execute("meme", List.of("never-saved")));
    }

    @Test
    void a_batch_removes_exactly_the_named_ids() {
        store.add("alice@example.com", "favourites", new ItemRef("comment", "c-1"));
        store.add("alice@example.com", "favourites", new ItemRef("comment", "c-2"));
        store.add("bob@example.com", "favourites", new ItemRef("comment", "c-2"));
        store.add("bob@example.com", "favourites", new ItemRef("comment", "c-3"));

        assertEquals(3, purge.execute("comment", List.of("c-1", "c-2")));

        assertTrue(store.list("alice@example.com", "favourites").isEmpty());
        assertEquals(List.of(new ItemRef("comment", "c-3")),
                store.list("bob@example.com", "favourites"),
                "c-3 was not in the event, so it stays");
    }

    @Test
    void blanks_and_duplicates_in_one_event_cost_nothing() {
        // an at-least-once producer merging two batches may well name the same comment twice,
        // and a null in the array is a producer bug we absorb rather than crash on
        store.add("alice@example.com", "favourites", new ItemRef("comment", "c-1"));

        assertEquals(1, purge.execute("comment", withNull("c-1", "c-1", "", "  ")),
                "the ref goes exactly once, and the noise is dropped before the store sees it");
    }

    @Test
    void nothing_worth_purging_is_zero_rather_than_an_empty_statement() {
        // the guard that keeps a degenerate event out of the adapter: an IN () list is not valid
        // SQL, so this must never reach JdbcCollectionStore
        assertEquals(0, purge.execute("meme", List.of()));
        assertEquals(0, purge.execute("meme", Collections.singletonList(null)));
        assertEquals(0, purge.execute("meme", null));
        assertEquals(0, purge.execute("", List.of("m-1")));
        assertEquals(0, purge.execute(null, List.of("m-1")));
    }

    /** A list that really contains a null — {@code List.of} refuses one. */
    private static List<String> withNull(String... ids) {
        List<String> withANull = new ArrayList<>(Arrays.asList(ids));
        withANull.add(null);
        return withANull;
    }

    private static List<ItemRef> sorted(List<ItemRef> refs) {
        List<ItemRef> copy = new ArrayList<>(refs);
        copy.sort((a, b) -> (a.itemType() + a.itemId()).compareTo(b.itemType() + b.itemId()));
        return copy;
    }
}
