-- The backlog alarm's own index, which V3 believed it had already created.
--
-- V3's idx_collection_items_erasure is (user_email, status, marked_for_erasure_at) and its header
-- says the instant "rides along for the backlog alarm, which asks the same table the other way
-- round". It does not ride along: the alarm's only query (JdbcItemErasure#pendingSince) constrains
-- the status and the instant and names NO user, so the leading column of that index is
-- unconstrained and the planner cannot use it as a lookup. The erasure-backlog-watch thread
-- therefore scanned the whole table, once a minute, for the lifetime of the process — and the
-- migration asserted the opposite, so nobody reading it would look.
--
-- Two columns in the order the question asks them: the status is an equality, the instant a range.
-- V3's index stays exactly as it is — it answers the OTHER direction (everything of ONE leaver:
-- the mark, the compensation and the closure), which is the frequent one. That is the same pair
-- both siblings keep, for the same two questions (idx_memes_pending_erasure,
-- idx_comments_pending_erasure beside their author index).
CREATE INDEX idx_collection_items_pending_erasure
    ON collection_items (status, marked_for_erasure_at);
