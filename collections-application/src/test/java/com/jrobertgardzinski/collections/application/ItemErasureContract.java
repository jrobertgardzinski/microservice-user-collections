package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.SavedItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@link ItemErasure} promises, asked of EVERY implementation — the JDBC adapter the service
 * runs on and each stand-in used in its place. This port has FOUR of them, which is three more
 * chances to drift.
 *
 * <p>A stand-in that drifts does not fail; it makes a green suite say something about a service
 * that does not exist. Both drifts this contract was written after were invisible: stand-ins
 * answering {@link ItemErasure#pendingSince} inclusively where the adapter asks
 * {@code marked_for_erasure_at < ?}, and stand-ins INVENTING a row on {@code store} where the
 * adapter runs an {@code UPDATE … WHERE} that quietly matches nothing.
 *
 * <p>Fresh names per test method, because one implementation is a database other suites share.
 */
public abstract class ItemErasureContract {

    private static final Instant NOON = Instant.parse("2026-09-24T12:00:00Z");
    private static final String LIST = "favourites";

    private final String run = UUID.randomUUID().toString().substring(0, 8);
    private final String alice = "alice+" + run + "@example.com";
    private final String bob = "bob+" + run + "@example.com";

    protected abstract ItemErasure erasure();

    /** Put an ACTIVE saved reference in, however this implementation stores one. */
    protected abstract void givenSavedItem(String user, String collection, ItemRef ref);

    private ItemRef ref(String id) {
        return new ItemRef("meme", id + "-" + run);
    }

    private SavedItem theOnly(List<SavedItem> found) {
        assertEquals(1, found.size(), "expected exactly one saved reference, got " + found);
        return found.get(0);
    }

    @Test
    @DisplayName("active and pending are the two halves of one member's references")
    protected void active_and_pending_split_by_status() {
        givenSavedItem(alice, LIST, ref("one"));
        givenSavedItem(alice, LIST, ref("two"));
        givenSavedItem(bob, LIST, ref("three"));

        assertEquals(2, erasure().activeOf(alice).size());
        assertEquals(List.of(), erasure().pendingOf(alice));

        erasure().store(theOnly(erasure().activeOf(bob)).markForErasure(NOON));

        assertEquals(List.of(), erasure().activeOf(bob));
        assertEquals(NOON, theOnly(erasure().pendingOf(bob)).markedForErasureAt());
    }

    @Test
    @DisplayName("a restored reference is active again and carries no mark")
    protected void restoring_puts_it_back() {
        givenSavedItem(alice, LIST, ref("one"));
        erasure().store(theOnly(erasure().activeOf(alice)).markForErasure(NOON));

        erasure().store(theOnly(erasure().pendingOf(alice)).restore());

        assertEquals(null, theOnly(erasure().activeOf(alice)).markedForErasureAt());
        assertEquals(List.of(), erasure().pendingOf(alice));
    }

    @Test
    @DisplayName("store NEVER invents a row: the adapter updates by key and matches nothing")
    protected void store_of_something_nobody_holds_changes_nothing() {
        givenSavedItem(alice, LIST, ref("one"));

        // a reference this member never saved. The adapter runs UPDATE … WHERE, so this matches no
        // row and does nothing; a stand-in that appends instead would hand the specs a member who
        // owns something they never saved — and hide a closure that failed to erase it
        erasure().store(new SavedItem(alice, LIST, ref("never-saved"),
                theOnly(erasure().activeOf(alice)).markForErasure(NOON).status(), NOON));

        assertEquals(1, erasure().activeOf(alice).size() + erasure().pendingOf(alice).size(),
                "this port stores STATE for rows that exist; it does not create them");
    }

    @Test
    @DisplayName("pendingSince is STRICTLY before the cutoff — the adapter asks `marked_for_erasure_at < ?`")
    protected void pending_since_excludes_the_cutoff_itself() {
        givenSavedItem(alice, LIST, ref("one"));
        erasure().store(theOnly(erasure().activeOf(alice)).markForErasure(NOON));

        assertEquals(List.of(), erasure().pendingSince(NOON),
                "a reference marked AT the cutoff is not yet overdue");
        assertTrue(erasure().pendingSince(NOON.plusSeconds(1)).stream()
                        .anyMatch(item -> item.user().equals(alice)),
                "a reference marked before the cutoff is overdue");
    }
}
