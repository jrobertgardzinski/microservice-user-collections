package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.ItemErasureContractTest;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;

/**
 * The heap store the Gherkin scenarios run on, held to what the JDBC adapter promises. It is the
 * one stand-in in this estate that ships inside the service jar — its own javadoc says the running
 * service never uses it — so it is also the one most likely to be trusted by somebody who assumes
 * a production class was reviewed as production code.
 */
@Epic("Architecture")
@Feature("A stand-in behaves like the adapter it stands in for")
class InMemoryCollectionStoreTest extends ItemErasureContractTest {

    private final InMemoryCollectionStore store = new InMemoryCollectionStore();

    @Override
    protected ItemErasure erasure() {
        return store;
    }

    @Override
    protected void givenSavedItem(String user, String collection, ItemRef ref) {
        store.add(user, collection, ref);
    }
}
