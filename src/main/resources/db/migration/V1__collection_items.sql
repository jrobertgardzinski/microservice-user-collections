-- One row per saved reference. The surrogate id gives a stable newest-first ordering (a wall-clock
-- added_at can tie within a millisecond); the UNIQUE constraint gives set semantics, which is what
-- makes a repeated save a no-op. The user_email index makes the account-deletion purge cheap.
CREATE TABLE collection_items (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_email VARCHAR(320) NOT NULL,
    collection VARCHAR(64)  NOT NULL,
    item_type  VARCHAR(64)  NOT NULL,
    item_id    VARCHAR(128) NOT NULL,
    added_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_collection_item UNIQUE (user_email, collection, item_type, item_id)
);

CREATE INDEX idx_collection_items_user ON collection_items (user_email);
