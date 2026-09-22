package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.UserItemsRekey;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Every column in this schema that holds a person's e-mail address, in one place — because the
 * value of writing them down together is that the next one cannot be forgotten quietly.
 *
 * <p>The list is one entry long. {@code collection_items.user_email} (V1) is the whole of this
 * service's idea of who a row belongs to: the API reads it, the cascade ignores it, the saga marks,
 * restores and erases by it. Nothing else here keys on a person — {@code collection}, {@code
 * item_type} and {@code item_id} are the reference itself, {@code status} and {@code
 * marked_for_erasure_at} are the saga's, and {@code id} and {@code added_at} are the row's own. Nor
 * is there an outbox to consider, which is where both Spring siblings had to stop and argue: this
 * participant's confirmation is the return value of {@code PurgeCommandsConsumer#handle} and is sent
 * straight away, so there is no table of already-built messages that a re-key could rewrite history
 * in.
 *
 * <p>{@code collection_items} and not {@code active_collection_items}: the view is a read of this
 * same column, so it follows for free — but more to the point, a reference a running saga has
 * already reserved must move with the rest, or the closure command (which deletes the leaver's
 * PENDING_ERASURE rows BY ADDRESS) would find nothing left to erase.
 *
 * <p>One plain UPDATE, with no upsert and no conflict handling, and that is a statement about the
 * data rather than an omission. The address takes part in exactly one unique constraint —
 * {@code uq_collection_item (user_email, collection, item_type, item_id)} — so a collision would
 * need the NEW address to already hold the same reference in the same collection, which cannot
 * happen: security refuses a move onto a registered address, and an unregistered one is either
 * untouched or had its owner's rows taken by the deletion that freed it. If that ever stopped being
 * true the statement fails with 23505 and the failure leaves this method as it is — the listener
 * does not commit the offset and retries the whole rename, which is the right failure. What must
 * never happen is the other reading of a duplicate, the one {@link JdbcCollectionStore#add} is
 * allowed to make: "already saved", a swallowed 23505 and a reference silently left behind under
 * the old address.
 */
public class JdbcUserItemsRekey implements UserItemsRekey {

    private final DataSource dataSource;

    public JdbcUserItemsRekey(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public int moveTo(String oldEmail, String newEmail) {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE collection_items SET user_email = ? WHERE user_email = ?")) {
            ps.setString(1, newEmail);
            ps.setString(2, oldEmail);
            // one statement, so the whole move is one transaction whatever the connection's
            // autocommit says — the person's references arrive under the new address together or
            // not at all
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not re-key a member's saved references", e);
        }
    }
}
