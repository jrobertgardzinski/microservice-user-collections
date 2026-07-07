package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.CollectionStore;
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
 */
public class JdbcCollectionStore implements CollectionStore {

    private static final String UNIQUE_VIOLATION = "23505";

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
}
