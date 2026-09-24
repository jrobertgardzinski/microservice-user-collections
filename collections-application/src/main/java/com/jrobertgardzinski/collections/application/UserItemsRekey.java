package com.jrobertgardzinski.collections.application;

/**
 * Moving everything this service holds of one person from the address they had to the address they
 * have.
 *
 * <p>A port of its own rather than a method on {@link CollectionStore} or {@link ItemErasure}, and
 * for once the reason is not the usual one about two questions: it is that the question belongs to
 * NEITHER of them. {@link CollectionStore} is the owner's world and may not see a reserved row;
 * {@link ItemErasure} is the saga's and sees only those. A rename is about a PERSON and has to move
 * every row keyed by their address whatever a saga is doing to it, so it would have to break one of
 * those two promises to live in either.
 *
 * <p>The count is the row count, for the log line and for a reader's sense of scale — nothing
 * decides anything on it. Zero is an ordinary answer: the person may have saved nothing, and a
 * redelivered rename finds the rows already moved.
 */
public interface UserItemsRekey {

    /** Every row keyed by {@code oldEmail}, re-keyed to {@code newEmail}; returns how many moved. */
    int moveTo(String oldEmail, String newEmail);
}
