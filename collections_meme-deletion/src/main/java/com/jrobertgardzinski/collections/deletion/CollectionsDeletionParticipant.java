package com.jrobertgardzinski.collections.deletion;

import com.jrobertgardzinski.collections.system.PurgeDeletedItem;
import com.jrobertgardzinski.deletion.CommentsDeleted;
import com.jrobertgardzinski.deletion.DeletionOutcome;
import com.jrobertgardzinski.deletion.MemeDeleted;

import java.util.List;

/**
 * This service's hop of the deletion cascade, and the cascade's END: it hears both announcements
 * and makes none of its own. No collection may go on pointing at a meme the portal no longer has,
 * or at a comment that went down with it.
 *
 * <p>No unit of work either, for the reason there is no announcement: one purge is one statement,
 * and there is no second write that would have to share its fate.
 */
public class CollectionsDeletionParticipant {

    /** The two item types this service's refs use for the two cascading sources. */
    public static final String MEME_ITEM_TYPE = "meme";
    public static final String COMMENT_ITEM_TYPE = "comment";

    private final PurgeDeletedItem purgeDeletedItem;

    public CollectionsDeletionParticipant(PurgeDeletedItem purgeDeletedItem) {
        this.purgeDeletedItem = purgeDeletedItem;
    }

    /** The meme itself is gone: every reference to it goes. */
    public DeletionOutcome handle(MemeDeleted memeDeleted) {
        return outcomeOf(purgeDeletedItem.execute(MEME_ITEM_TYPE, List.of(memeDeleted.memeId())));
    }

    /**
     * The meme's thread went with it: every reference to those comments goes. An event that names
     * no usable comment drops nothing — a valid thing for the cascade to say, and nothing to do.
     */
    public DeletionOutcome handle(CommentsDeleted commentsDeleted) {
        return outcomeOf(purgeDeletedItem.execute(COMMENT_ITEM_TYPE, commentsDeleted.commentIds()));
    }

    private static DeletionOutcome outcomeOf(int removed) {
        return removed == 0 ? new DeletionOutcome.Nothing() : new DeletionOutcome.Dropped(removed);
    }
}
