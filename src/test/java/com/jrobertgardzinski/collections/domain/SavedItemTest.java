package com.jrobertgardzinski.collections.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The third copy of the same three rules (memes and comments have theirs), and it earns its keep
 * for the same reason: these are the decisions everything else merely carries, and two of them —
 * "a redelivery keeps the FIRST instant" and "the illegal combination cannot be built" — are
 * invisible to every test that runs above this record.
 */
class SavedItemTest {

    private static final Instant FIRST_DELIVERY = Instant.parse("2026-08-08T10:00:00Z");
    private static final Instant REDELIVERY = FIRST_DELIVERY.plus(Duration.ofHours(1));

    private static SavedItem inTheList() {
        return new SavedItem("leaver@example.com", "favourites", new ItemRef("meme", "42"));
    }

    @Test
    @DisplayName("a fresh reference is ACTIVE and carries no mark")
    void the_shorthand_constructor_is_the_list_state() {
        SavedItem item = inTheList();

        assertEquals(ItemStatus.ACTIVE, item.status());
        assertEquals(null, item.markedForErasureAt());
        assertFalse(item.isPendingErasure());
    }

    @Test
    @DisplayName("marking reserves the reference and records when")
    void mark_sets_the_status_and_the_instant_together() {
        SavedItem marked = inTheList().markForErasure(FIRST_DELIVERY);

        assertTrue(marked.isPendingErasure());
        assertEquals(FIRST_DELIVERY, marked.markedForErasureAt());
        assertEquals(new ItemRef("meme", "42"), marked.ref(), "the ref itself is untouched");
    }

    @Test
    @DisplayName("a redelivered mark keeps the FIRST instant — the backlog measures an age")
    void marking_twice_does_not_rejuvenate_the_obligation() {
        SavedItem marked = inTheList().markForErasure(FIRST_DELIVERY);

        SavedItem again = marked.markForErasure(REDELIVERY);

        assertEquals(FIRST_DELIVERY, again.markedForErasureAt());
        assertSame(marked, again);
    }

    @Test
    @DisplayName("restoring puts it back exactly as it was, and twice is once")
    void restore_is_the_inverse_and_is_idempotent() {
        SavedItem item = inTheList();

        SavedItem restored = item.markForErasure(FIRST_DELIVERY).restore();

        assertEquals(item, restored);
        assertSame(restored, restored.restore());
    }

    @Test
    @DisplayName("restoring a reference nobody marked is a no-op, not an error")
    void restore_of_an_unmarked_item_does_not_throw() {
        SavedItem item = inTheList();

        assertSame(item, item.restore());
    }

    @Test
    @DisplayName("a mark without its instant — or an instant without its mark — cannot be built")
    void the_invariant_is_unrepresentable_not_merely_discouraged() {
        ItemRef ref = new ItemRef("meme", "42");
        assertThrows(IllegalArgumentException.class,
                () -> new SavedItem("a@b.c", "favourites", ref, ItemStatus.PENDING_ERASURE, null));
        assertThrows(IllegalArgumentException.class,
                () -> new SavedItem("a@b.c", "favourites", ref, ItemStatus.ACTIVE, FIRST_DELIVERY));
    }
}
