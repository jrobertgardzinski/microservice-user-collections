-- The last of the three saga participants learns to hide before it deletes (ADR 0007).
--
-- WHAT WAS WRONG, and it was written down as a known debt rather than discovered: memes and
-- comments were converted first, so a saga that was given up on restored the leaver's pictures and
-- their words — and not the list of things they had saved, which this service had already deleted
-- on the very first command. The leaver got their account back with a favourites list silently
-- emptied, and a private list is the one loss nobody else can see.
--
-- HOW THIS FIXES IT, in exactly the shape the two siblings already have: two columns and one view.
-- PURGE_USER_CONTENT marks (status = PENDING_ERASURE, marked_for_erasure_at = now), the mark hides
-- the row from every listing because every listing reads active_collection_items,
-- RESTORE_USER_CONTENT flips it back, and only ERASE_USER_CONTENT deletes.
--
-- DELIBERATELY NOT a separate table of things to erase: the fact is a property OF THE ROW, with
-- exactly one lifetime, and a second table would need this row's key duplicated, its own cascade
-- (this service already has one, on the item axis), its own idempotence story and a join on every
-- listing.

ALTER TABLE collection_items ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE';
-- when the running saga reserved this reference; NULL for everything in somebody's list. Not "when
-- to delete it": there is no such moment, because nothing but the saga's closure may delete it.
ALTER TABLE collection_items ADD COLUMN marked_for_erasure_at TIMESTAMP;

ALTER TABLE collection_items ADD CONSTRAINT ck_collection_items_status
    CHECK (status IN ('ACTIVE', 'PENDING_ERASURE'));
ALTER TABLE collection_items ADD CONSTRAINT ck_collection_items_erasure_mark
    CHECK ((status = 'PENDING_ERASURE') = (marked_for_erasure_at IS NOT NULL));

-- THE one place the ACTIVE filter is written down. Every read that can reach a person selects from
-- HERE and never from collection_items, so a new query cannot forget the filter — there is no
-- filter to forget — and ItemReadFilterTest fails the build over a read that names the base table.
--
-- Columns are listed, not SELECT *: a view built with a star pins its column list at creation time
-- on Postgres, so the next ALTER TABLE would leave the view silently behind.
--
-- The UNIQUE constraint stays on the TABLE, not the view, and that is deliberate: while a saga has
-- a reference reserved, re-saving the same thing must not create a second row. It cannot happen
-- today (the account is locked for the whole deletion, so nobody can save anything), but a
-- constraint that depends on an assumption made in another service is a constraint waiting to be
-- broken. The insert simply hits 23505 and is read as "already saved", exactly as before.
CREATE VIEW active_collection_items AS
    SELECT id, user_email, collection, item_type, item_id, added_at
    FROM collection_items
    WHERE status = 'ACTIVE';

-- The reaper's whole data structure: the owner, the status and the instant. The leaver's reserved
-- rows (the erasure's DELETE and the compensation's UPDATE) are the frequent lookup; the instant
-- rides along for the backlog alarm, which asks the same table the other way round.
CREATE INDEX idx_collection_items_erasure
    ON collection_items (user_email, status, marked_for_erasure_at);
