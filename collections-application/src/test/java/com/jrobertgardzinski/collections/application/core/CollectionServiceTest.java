package com.jrobertgardzinski.collections.application.core;

import com.jrobertgardzinski.identity.UserId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The names a caller sends are refused here when no column could hold them — no use case runs. */
class CollectionServiceTest {

    private final CollectionService service = new CollectionService(null, null, null);

    @Test
    @DisplayName("each segment is held to its column's width, and the first one over it is named")
    void segments_over_their_width_are_refused() {
        UserId user = UserId.random();
        assertEquals(new CollectionService.Saving.TooLong("COLLECTION_TOO_LONG"),
                service.save(user, "c".repeat(CollectionService.MAX_COLLECTION_LENGTH + 1), "meme", "m1"));
        assertEquals(new CollectionService.Removal.TooLong("ITEM_TYPE_TOO_LONG"),
                service.remove(user, "favourites", "t".repeat(CollectionService.MAX_ITEM_TYPE_LENGTH + 1), "m1"));
        assertEquals(new CollectionService.Saving.TooLong("ITEM_ID_TOO_LONG"),
                service.save(user, "favourites", "meme", "i".repeat(CollectionService.MAX_ITEM_ID_LENGTH + 1)));
        assertEquals(new CollectionService.Listing.TooLong("COLLECTION_TOO_LONG"),
                service.list(user, "c".repeat(CollectionService.MAX_COLLECTION_LENGTH + 1)));
    }
}
