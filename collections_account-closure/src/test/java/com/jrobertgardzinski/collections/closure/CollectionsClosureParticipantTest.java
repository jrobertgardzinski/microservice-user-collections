package com.jrobertgardzinski.collections.closure;

import com.jrobertgardzinski.closure.ClosureCommand;
import com.jrobertgardzinski.closure.ClosureInitiator;
import com.jrobertgardzinski.closure.ClosureMessages;
import com.jrobertgardzinski.collections.application.CollectionStore;
import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.ItemReferences;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What happens to a person's saved references when their account closes — proven with no broker,
 * no database, no web server and no container. That is the whole reason this module exists: the
 * flow is readable and testable before anybody decides whether the portal is six services or one.
 *
 * <p>The store here is a hand-written one, not a mock, because the interesting answers are counts
 * and the counts come from real rows moving between states.
 */
@Epic("Saga")
@Feature("Account closure — the collections axis")
class CollectionsClosureParticipantTest {

    private static final String LEAVER = "leaver@example.com";
    private static final String SAGA = "7d9f9e2a-1f0a-4f6e-9a1b-2c3d4e5f6a7b";

    private final HeapStore store = new HeapStore();
    private final List<Observation> observed = new ArrayList<>();

    private final MarkUserItemsForErasure markForErasure =
            new MarkUserItemsForErasure(store, Clock.systemUTC());
    private final RestoreUserItems restoreUserItems = new RestoreUserItems(store);
    private final PurgeUserItems purgeUserItems = new PurgeUserItems(store);
    private final SaveItem saveItem = new SaveItem(store);

    private final CollectionsClosureParticipant participant = new CollectionsClosureParticipant(
            markForErasure, restoreUserItems, purgeUserItems, (Observations<Observation>) observed::add);

    private ClosureCommand command(String type) {
        return new ClosureCommand(type, SAGA, LEAVER, ClosureInitiator.SELF.wire(), Optional.empty());
    }

    private void saved(String... ids) {
        for (String id : ids) {
            saveItem.execute(LEAVER, "favourites", new ItemRef("meme", id));
        }
    }

    @Test
    @DisplayName("the whole flow: mark reserves, the closure destroys exactly what was reserved")
    void mark_then_close() {
        saved("1", "2", "3");

        assertEquals(new ClosureOutcome.Reserved(3),
                participant.handle(command(ClosureMessages.PURGE_USER_CONTENT)));
        assertEquals(new ClosureOutcome.Erased(3, 0),
                participant.handle(command(ClosureMessages.ERASE_USER_CONTENT)));
        assertEquals(0, store.all(LEAVER).size());
    }

    @Test
    @DisplayName("the compensation puts back exactly what the mark took out")
    void mark_then_compensate() {
        saved("1", "2");

        participant.handle(command(ClosureMessages.PURGE_USER_CONTENT));
        assertEquals(new ClosureOutcome.Restored(2),
                participant.handle(command(ClosureMessages.RESTORE_USER_CONTENT)));
        assertEquals(2, store.visible(LEAVER).size(), "the lists are whole again");
    }

    @Test
    @DisplayName("a reference saved AFTER the mark is left standing, counted, by the closure")
    void the_closure_can_come_up_short() {
        saved("1", "2");
        participant.handle(command(ClosureMessages.PURGE_USER_CONTENT));

        // the offline gate is blind to a revocation until the token expires, so this still lands
        saved("3");

        assertEquals(new ClosureOutcome.Erased(2, 1),
                participant.handle(command(ClosureMessages.ERASE_USER_CONTENT)));
        assertTrue(observed.stream().anyMatch(o -> o instanceof Observation.ErasureResidue),
                "no command of this saga may destroy it and none will ever come, so it has to be "
                        + "countable rather than swept");
    }

    @Test
    @DisplayName("a mark that reserved nothing is still an answer — and is observed")
    void a_mark_that_found_nothing_is_observed() {
        assertEquals(new ClosureOutcome.Reserved(0),
                participant.handle(command(ClosureMessages.PURGE_USER_CONTENT)));

        assertTrue(observed.stream().anyMatch(o -> o instanceof Observation.PurgeReservedNothing),
                "\"I hold nothing of theirs\" and \"their rows are under an address they changed\" "
                        + "look identical from in here, so the silence has to be countable");
    }

    @Test
    @DisplayName("a command that names nobody is dropped, and nothing of anyone else's moves")
    void a_command_without_an_address_does_nothing() {
        saved("1");

        ClosureOutcome outcome = participant.handle(new ClosureCommand(
                ClosureMessages.PURGE_USER_CONTENT, SAGA, "   ", ClosureInitiator.SELF.wire(),
                Optional.empty()));

        assertInstanceOf(ClosureOutcome.Unaddressed.class, outcome);
        assertEquals(1, store.visible(LEAVER).size());
    }

    @Test
    @DisplayName("a command for somebody else's axis is ignored, not failed")
    void another_participants_command_is_ignored() {
        saved("1");

        assertEquals(new ClosureOutcome.NotOurs("MEMES_ANONYMISED"),
                participant.handle(new ClosureCommand("MEMES_ANONYMISED", SAGA, LEAVER,
                        ClosureInitiator.SELF.wire(), Optional.empty())));
        assertEquals(1, store.visible(LEAVER).size());
    }

    @Test
    @DisplayName("the leaver's conditions are never read: this axis has nothing to keep")
    void the_policy_has_no_meaning_here() {
        saved("1");
        participant.handle(command(ClosureMessages.PURGE_USER_CONTENT));

        // an ADMIN closure stating a rule — the other two participants would honour it
        participant.handle(new ClosureCommand(ClosureMessages.ERASE_USER_CONTENT, SAGA, LEAVER,
                ClosureInitiator.ADMIN.wire(), Optional.of("KEEP_POPULAR_ANONYMIZED:10")));

        assertEquals(0, store.all(LEAVER).size(), "a pointer has nothing to anonymise or keep");
    }
}
