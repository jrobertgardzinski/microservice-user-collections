package com.jrobertgardzinski.collections.appsteps;

import com.jrobertgardzinski.collections.domain.Observation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.observation.Observations;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.MarkUserItemsForErasure;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RestoreUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.InMemoryCollectionStore;
import com.jrobertgardzinski.collections.infrastructure.PurgeCommandsConsumer;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drives the use cases over an in-memory store — the application-layer entry point. */
public class CollectionsSteps {

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final SaveItem saveItem = new SaveItem(store);
    private final RemoveItem removeItem = new RemoveItem(store);
    private final ListItems listItems = new ListItems(store);
    private final MarkUserItemsForErasure markForErasure =
            new MarkUserItemsForErasure(store, java.time.Clock.systemUTC());
    private final RestoreUserItems restoreUserItems = new RestoreUserItems(store);
    private final PurgeUserItems purgeUserItems = new PurgeUserItems(store);
    private final PurgeCommandsConsumer purgeConsumer = new PurgeCommandsConsumer(
            markForErasure, restoreUserItems, purgeUserItems, new ObjectMapper(), Observations.<Observation>silent());

    private static final String SAGA_ID = "saga-1";

    private SaveItem.Status lastSave;
    private RemoveItem.Status lastRemove;
    private Optional<String> lastConfirmation;

    @When("^(\\w+) saves (\\w+) (\\d+) into \"([^\"]+)\"$")
    @Given("^(\\w+) has saved (\\w+) (\\d+) into \"([^\"]+)\"$")
    public void saves(String user, String type, String id, String collection) {
        lastSave = saveItem.execute(user, collection, new ItemRef(type, id));
    }

    @When("^(\\w+) removes (\\w+) (\\d+) from \"([^\"]+)\"$")
    public void removes(String user, String type, String id, String collection) {
        lastRemove = removeItem.execute(user, collection, new ItemRef(type, id));
    }

    @When("^(\\w+)'s account is purged$")
    public void accountPurged(String user) {
        // Through the CONSUMER, not straight into the use case. The feature calls these scenarios
        // "Kafka-borne" and HttpBddTest's javadoc says the purge "arrives over Kafka" — but this
        // step used to call purgeUserItems.execute directly, skipping the command parsing and the
        // confirmation entirely. Its neighbour, the negative scenario, DID go through the consumer,
        // so the suite proved the refusal of a malformed command and never the acceptance of a good
        // one. Living documentation that overstates its own reach is the failure this review kept
        // finding, and it is worst here, where the docs name the transport.
        lastConfirmation = purgeConsumer.handle("{\"type\":\"PURGE_USER_CONTENT\",\"email\":\""
                + user + "\",\"sagaId\":\"" + SAGA_ID + "\"}");
    }

    @When("^the ORCHESTRATOR compensates the SAGA$")
    public void orchestratorCompensates() {
        // RESTORE_USER_CONTENT: sent when a SIBLING participant failed. This service is not the one
        // that failed and does not get to decide — it only obeys
        lastConfirmation = purgeConsumer.handle("{\"type\":\"RESTORE_USER_CONTENT\",\"email\":\""
                + "alice\",\"sagaId\":\"" + SAGA_ID + "\"}");
    }

    @When("^the ORCHESTRATOR closes the SAGA$")
    public void orchestratorClosesTheSaga() {
        // ERASE_USER_CONTENT: the closure, and the only command that destroys anything here
        lastConfirmation = purgeConsumer.handle("{\"type\":\"ERASE_USER_CONTENT\",\"email\":\""
                + "alice\",\"sagaId\":\"" + SAGA_ID + "\"}");
    }

    @Then("^a late compensation brings nothing back$")
    public void lateCompensationBringsNothingBack() {
        // the orchestrator never sends this after a closure — its state machine forbids it — but if
        // one did arrive, past the closure there is nothing reserved to restore and nothing to throw
        orchestratorCompensates();
        assertTrue(store.list("alice", "favourites").isEmpty(),
                "erased is erased: a compensation cannot resurrect what the closure deleted");
    }

    @When("^a purge command arrives naming nobody$")
    public void purgeCommandNamingNobody() {
        lastConfirmation = purgeConsumer.handle(
                "{\"type\":\"PURGE_USER_CONTENT\",\"email\":\"\",\"sagaId\":\"saga-nobody\"}");
    }

    @Then("^a CONFIRMATION for that SAGA goes back to the ORCHESTRATOR$")
    public void confirmationGoesBack() throws Exception {
        // the orchestrator matches on both: the type tells it which participant answered, the saga
        // id which run — a confirmation carrying the wrong one is as useless as none at all
        String confirmation = lastConfirmation.orElseThrow(
                () -> new AssertionError("the orchestrator is left waiting for an answer that came"));
        var body = new ObjectMapper().readTree(confirmation);
        assertEquals("USER_CONTENT_PURGED", body.path("type").asText());
        assertEquals(SAGA_ID, body.path("sagaId").asText());
    }

    @Then("^no CONFIRMATION goes back to the ORCHESTRATOR$")
    public void noConfirmationGoesBack() {
        assertTrue(lastConfirmation.isEmpty(),
                "a confirmation would claim a purge happened that never did");
    }

    @Then("^the save reports it was already there$")
    public void saveWasIdempotent() {
        assertEquals(SaveItem.Status.ALREADY_SAVED, lastSave);
    }

    @Then("^the removal reports it was not there$")
    public void removalWasIdempotent() {
        assertEquals(RemoveItem.Status.NOT_SAVED, lastRemove);
    }

    @Then("^(\\d+) REFERENCES were reserved$")
    public void referencesRemoved(int count) throws Exception {
        // the number the MARK put on the wire, not one the steps kept for themselves. This used to
        // compare the Feature's figure against a tally the save and remove steps maintained, so it
        // passed for whatever the mark had really reserved — zero included — and observed nothing
        // the production code does. The confirmation is where that count is stated, and the
        // orchestrator reads the same field
        String confirmation = lastConfirmation.orElseThrow(
                () -> new AssertionError("nothing was confirmed, so nothing said what was reserved"));
        assertEquals(count, new ObjectMapper().readTree(confirmation).path("reserved").asInt(),
                "the participant tells the orchestrator how many references it took out of the"
                        + " member's lists, and that is the number this Feature names");
    }

    @Then("^(\\w+)'s \"([^\"]+)\" contains (\\w+) (\\d+)$")
    public void contains(String user, String collection, String type, String id) {
        assertTrue(listItems.execute(user, collection).contains(new ItemRef(type, id)));
    }

    @Then("^(\\w+)'s \"([^\"]+)\" contains (\\w+) (\\d+) once$")
    public void containsOnce(String user, String collection, String type, String id) {
        long times = listItems.execute(user, collection).stream()
                .filter(new ItemRef(type, id)::equals).count();
        assertEquals(1, times);
    }

    @Then("^(\\w+)'s \"([^\"]+)\" lists (\\w+) (\\d+) then (\\w+) (\\d+)$")
    public void listsInOrder(String user, String collection,
                             String firstType, String firstId, String secondType, String secondId) {
        List<ItemRef> items = listItems.execute(user, collection);
        assertEquals(new ItemRef(firstType, firstId), items.get(0));
        assertEquals(new ItemRef(secondType, secondId), items.get(1));
    }

    @Then("^(\\w+)'s \"([^\"]+)\" is empty$")
    public void isEmpty(String user, String collection) {
        assertTrue(listItems.execute(user, collection).isEmpty());
    }
}
