package com.jrobertgardzinski.collections;

import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.PurgeUserItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import com.jrobertgardzinski.collections.infrastructure.InMemoryCollectionStore;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drives the use cases over an in-memory store — the application-layer entry point. */
public class CollectionsSteps {

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();
    private final SaveItem saveItem = new SaveItem(store);
    private final RemoveItem removeItem = new RemoveItem(store);
    private final ListItems listItems = new ListItems(store);
    private final PurgeUserItems purgeUserItems = new PurgeUserItems(store);

    private SaveItem.Status lastSave;
    private RemoveItem.Status lastRemove;
    private int lastPurgeCount;

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
        lastPurgeCount = purgeUserItems.execute(user);
    }

    @Then("^the save reports it was already there$")
    public void saveWasIdempotent() {
        assertEquals(SaveItem.Status.ALREADY_SAVED, lastSave);
    }

    @Then("^the removal reports it was not there$")
    public void removalWasIdempotent() {
        assertEquals(RemoveItem.Status.NOT_SAVED, lastRemove);
    }

    @Then("^(\\d+) references were removed$")
    public void referencesRemoved(int count) {
        assertEquals(count, lastPurgeCount);
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
