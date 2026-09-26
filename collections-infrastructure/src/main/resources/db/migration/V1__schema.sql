-- The whole schema, in one file: nothing is deployed anywhere, so there is no history to replay.
-- A change edits this file; a running dev database is recreated (docker compose -p security down -v).

CREATE TABLE collection_items (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_email            VARCHAR(320) NOT NULL,
    collection            VARCHAR(64)  NOT NULL,
    item_type             VARCHAR(64)  NOT NULL,
    item_id               VARCHAR(128) NOT NULL,
    added_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- the account-closure saga's reversible mark: out of every list, destroyed by nothing but the closure
    status                VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    marked_for_erasure_at TIMESTAMP,
    CONSTRAINT uq_collection_item UNIQUE (user_email, collection, item_type, item_id),
    CONSTRAINT ck_collection_items_status CHECK (status IN ('ACTIVE', 'PENDING_ERASURE')),
    CONSTRAINT ck_collection_items_erasure_mark
        CHECK ((status = 'PENDING_ERASURE') = (marked_for_erasure_at IS NOT NULL))
);
CREATE INDEX idx_collection_items_user ON collection_items (user_email);
CREATE INDEX idx_collection_items_item ON collection_items (item_type, item_id);
CREATE INDEX idx_collection_items_erasure ON collection_items (user_email, status, marked_for_erasure_at);
CREATE INDEX idx_collection_items_pending_erasure ON collection_items (status, marked_for_erasure_at);

-- every public read goes through the view and never sees a marked reference
CREATE VIEW active_collection_items AS
    SELECT id, user_email, collection, item_type, item_id, added_at
    FROM collection_items
    WHERE status = 'ACTIVE';
