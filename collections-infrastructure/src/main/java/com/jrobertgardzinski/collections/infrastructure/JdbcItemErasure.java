package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.domain.ItemErasure;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.ItemStatus;
import com.jrobertgardzinski.collections.domain.SavedItem;
import com.jrobertgardzinski.identity.UserId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The one adapter in this service allowed to name the {@code collection_items} table in a READ —
 * seeing marked rows is its entire job. Everything else goes through the
 * {@code active_collection_items} view, and {@code ItemReadFilterTest} fails the build if that
 * stops being true. Rows are addressed by their natural key (user_id, collection, item_type,
 * item_id), the one the schema makes UNIQUE.
 */
public class JdbcItemErasure implements ItemErasure {

    private final DataSource dataSource;

    public JdbcItemErasure(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<SavedItem> activeOf(UserId user) {
        return byUserAndStatus(user, ItemStatus.ACTIVE);
    }

    @Override
    public List<SavedItem> pendingOf(UserId user) {
        return byUserAndStatus(user, ItemStatus.PENDING_ERASURE);
    }

    private List<SavedItem> byUserAndStatus(UserId user, ItemStatus status) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT user_id, collection, item_type, item_id, status, marked_for_erasure_at "
                             + "FROM collection_items WHERE user_id = ? AND status = ?")) {
            ps.setObject(1, user.value());
            ps.setString(2, status.name());
            try (ResultSet rs = ps.executeQuery()) {
                List<SavedItem> found = new ArrayList<>();
                while (rs.next()) {
                    found.add(itemOf(rs));
                }
                return found;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not read the erasure state of a user's items", e);
        }
    }

    @Override
    public void store(SavedItem state) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE collection_items SET status = ?, marked_for_erasure_at = ? "
                             + "WHERE user_id = ? AND collection = ? AND item_type = ? "
                             + "AND item_id = ?")) {
            ps.setString(1, state.status().name());
            ps.setTimestamp(2, state.markedForErasureAt() == null
                    ? null : Timestamp.from(state.markedForErasureAt()));
            ps.setObject(3, state.user().value());
            ps.setString(4, state.collection());
            ps.setString(5, state.ref().itemType());
            ps.setString(6, state.ref().itemId());
            // no check on the update count: a row that is gone is not an error. The deletion
            // cascade removes references to a deleted meme or comment whatever their status, and a
            // reference whose target no longer exists has nothing to be restored to
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not store the erasure state of an item", e);
        }
    }

    @Override
    public int eraseMarked(UserId user) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM collection_items WHERE user_id = ? AND status = ?")) {
            ps.setObject(1, user.value());
            // the status in the WHERE clause is the whole exactness of the closure: whatever was
            // saved after the mark was never part of this saga and is not the closure's business
            ps.setString(2, ItemStatus.PENDING_ERASURE.name());
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not erase the marked items of a user", e);
        }
    }

    @Override
    public List<SavedItem> pendingSince(Instant cutoff) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT user_id, collection, item_type, item_id, status, "
                             + "marked_for_erasure_at FROM collection_items "
                             + "WHERE status = ? AND marked_for_erasure_at < ?")) {
            ps.setString(1, ItemStatus.PENDING_ERASURE.name());
            ps.setTimestamp(2, Timestamp.from(cutoff));
            try (ResultSet rs = ps.executeQuery()) {
                List<SavedItem> stuck = new ArrayList<>();
                while (rs.next()) {
                    stuck.add(itemOf(rs));
                }
                return stuck;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not read the erasure backlog", e);
        }
    }

    private static SavedItem itemOf(ResultSet rs) throws SQLException {
        Timestamp marked = rs.getTimestamp("marked_for_erasure_at");
        return new SavedItem(new UserId(rs.getObject("user_id", UUID.class)), rs.getString("collection"),
                new ItemRef(rs.getString("item_type"), rs.getString("item_id")),
                ItemStatus.valueOf(rs.getString("status")),
                marked == null ? null : marked.toInstant());
    }
}
