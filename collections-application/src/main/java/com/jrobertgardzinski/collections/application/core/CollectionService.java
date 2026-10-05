package com.jrobertgardzinski.collections.application.core;

import com.jrobertgardzinski.collections.domain.core.ItemRef;
import com.jrobertgardzinski.collections.system.core.ListItems;
import com.jrobertgardzinski.collections.system.core.RemoveItem;
import com.jrobertgardzinski.collections.system.core.SaveItem;
import com.jrobertgardzinski.identity.UserId;

import java.util.List;

/**
 * A signed-in user's collections from the outside: saving a reference, removing one, listing a
 * collection. The caller arrives already proven; the names they sent become the domain here.
 */
public final class CollectionService {

    // the schema's column widths (V1__schema.sql): anything longer would only surface as a
    // SQLException deep in the JDBC store, so it is refused here, before any use case runs
    static final int MAX_COLLECTION_LENGTH = 64;
    static final int MAX_ITEM_TYPE_LENGTH = 64;
    static final int MAX_ITEM_ID_LENGTH = 128;

    private final SaveItem saveItem;
    private final RemoveItem removeItem;
    private final ListItems listItems;

    public CollectionService(SaveItem saveItem, RemoveItem removeItem, ListItems listItems) {
        this.saveItem = saveItem;
        this.removeItem = removeItem;
        this.listItems = listItems;
    }

    public Saving save(UserId user, String collection, String itemType, String itemId) {
        String tooLong = tooLong(collection, itemType, itemId);
        if (tooLong != null) {
            return new Saving.TooLong(tooLong);
        }
        return switch (saveItem.execute(user, collection, new ItemRef(itemType, itemId))) {
            case SAVED -> new Saving.Saved();
            case ALREADY_SAVED -> new Saving.AlreadySaved();
        };
    }

    public Removal remove(UserId user, String collection, String itemType, String itemId) {
        String tooLong = tooLong(collection, itemType, itemId);
        if (tooLong != null) {
            return new Removal.TooLong(tooLong);
        }
        return switch (removeItem.execute(user, collection, new ItemRef(itemType, itemId))) {
            case REMOVED -> new Removal.Removed();
            case NOT_SAVED -> new Removal.NotSaved();
        };
    }

    public Listing list(UserId user, String collection) {
        if (collection.length() > MAX_COLLECTION_LENGTH) {
            return new Listing.TooLong("COLLECTION_TOO_LONG");
        }
        return new Listing.Listed(listItems.execute(user, collection));
    }

    /** The code of the first segment over its column's width, or null when all fit. */
    private static String tooLong(String collection, String itemType, String itemId) {
        if (collection.length() > MAX_COLLECTION_LENGTH) {
            return "COLLECTION_TOO_LONG";
        }
        if (itemType.length() > MAX_ITEM_TYPE_LENGTH) {
            return "ITEM_TYPE_TOO_LONG";
        }
        if (itemId.length() > MAX_ITEM_ID_LENGTH) {
            return "ITEM_ID_TOO_LONG";
        }
        return null;
    }

    public sealed interface Saving {
        record Saved() implements Saving {}

        record AlreadySaved() implements Saving {}

        record TooLong(String code) implements Saving {}
    }

    public sealed interface Removal {
        record Removed() implements Removal {}

        record NotSaved() implements Removal {}

        record TooLong(String code) implements Removal {}
    }

    public sealed interface Listing {
        record Listed(List<ItemRef> items) implements Listing {}

        record TooLong(String code) implements Listing {}
    }
}
