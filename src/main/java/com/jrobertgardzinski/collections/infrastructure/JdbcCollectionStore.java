package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ItemReferences;
import com.jrobertgardzinski.collections.domain.ItemRef;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The durable {@link CollectionStore}: rows in {@code collection_items}, served by Postgres in prod
 * and H2 (PostgreSQL mode) in dev/tests through the same SQL. Saving is made idempotent by the
 * UNIQUE constraint — a duplicate insert raises SQLState 23505, which both engines use, and we read
 * that as "already saved" rather than an error.
 *
 * <p>It serves {@link ItemReferences} too — the same table read along its OTHER axis (see V2's
 * index). One adapter for one table; the ports stay two because the questions, the indexes and the
 * guarantees are two.
 */
public class JdbcCollectionStore implements CollectionStore, ItemReferences {

    private static final String UNIQUE_VIOLATION = "23505";

    /**
     * How many ids one {@code IN (...)} may carry. A COMMENTS_DELETED for a busy thread can name
     * thousands of comments in one event, and a single statement with one bind parameter per id
     * would walk towards Postgres' hard ceiling of 65535 parameters (and towards a plan the
     * planner stops indexing). Chunking keeps every statement small and predictable; the cost is
     * a handful of round trips on one borrowed connection instead of one enormous statement.
     */
    private static final int MAX_IDS_PER_STATEMENT = 500;

    private final DataSource dataSource;

    public JdbcCollectionStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public boolean add(String user, String collection, ItemRef item) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO collection_items (user_email, collection, item_type, item_id) "
                             + "VALUES (?, ?, ?, ?)")) {
            ps.setString(1, user);
            ps.setString(2, collection);
            ps.setString(3, item.itemType());
            ps.setString(4, item.itemId());
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION.equals(e.getSQLState())) {
                return false;   // already saved — idempotent
            }
            throw new IllegalStateException("could not save item", e);
        }
    }

    @Override
    public boolean remove(String user, String collection, ItemRef item) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM collection_items WHERE user_email = ? AND collection = ? "
                             + "AND item_type = ? AND item_id = ?")) {
            ps.setString(1, user);
            ps.setString(2, collection);
            ps.setString(3, item.itemType());
            ps.setString(4, item.itemId());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("could not remove item", e);
        }
    }

    @Override
    public List<ItemRef> list(String user, String collection) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT item_type, item_id FROM collection_items WHERE user_email = ? "
                             + "AND collection = ? ORDER BY id DESC")) {
            ps.setString(1, user);
            ps.setString(2, collection);
            try (ResultSet rs = ps.executeQuery()) {
                List<ItemRef> items = new ArrayList<>();
                while (rs.next()) {
                    items.add(new ItemRef(rs.getString(1), rs.getString(2)));
                }
                return items;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not list items", e);
        }
    }

    @Override
    public int purgeUser(String user) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM collection_items WHERE user_email = ?")) {
            ps.setString(1, user);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not purge user", e);
        }
    }

    /**
     * The item axis: one DELETE per chunk of ids, all on one borrowed connection. Deliberately
     * NOT wrapped in a single transaction across the chunks — the cascade is best-effort and its
     * accepted worst case is a leftover dead row, so a half-finished purge is a smaller problem
     * than a long write transaction holding locks on rows belonging to thousands of unrelated
     * users while the other chunks run. The count returned is what actually went.
     */
    @Override
    public int purge(String itemType, List<String> itemIds) {
        try (Connection c = dataSource.getConnection()) {
            int removed = 0;
            for (int from = 0; from < itemIds.size(); from += MAX_IDS_PER_STATEMENT) {
                removed += purgeChunk(c, itemType,
                        itemIds.subList(from, Math.min(from + MAX_IDS_PER_STATEMENT, itemIds.size())));
            }
            return removed;
        } catch (SQLException e) {
            throw new IllegalStateException("could not purge references to a deleted item", e);
        }
    }

    /** One DELETE over at most {@link #MAX_IDS_PER_STATEMENT} ids — the V2 index's lookup. */
    private static int purgeChunk(Connection c, String itemType, List<String> ids)
            throws SQLException {
        // the placeholders are generated from the COUNT of ids, never from their content: the ids
        // arrive off a Kafka topic, so they may never reach the SQL text itself
        String placeholders = "?, ".repeat(ids.size() - 1) + "?";
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM collection_items WHERE item_type = ? AND item_id IN ("
                        + placeholders + ")")) {
            ps.setString(1, itemType);
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 2, ids.get(i));
            }
            return ps.executeUpdate();
        }
    }
}
