package com.jrobertgardzinski.collections.application;

import com.jrobertgardzinski.collections.domain.ItemRef;

/**
 * The heap store the Gherkin scenarios (and account-closure-specs, via this module's test-jar) run
 * on, held to what the JDBC adapter promises. It used to ship inside the service jar in
 * collections-infrastructure — the one stand-in in the estate most likely to be mistaken for
 * reviewed production code, since its own javadoc had to say the running service never uses it.
 * Living here, on the test classpath next to {@link ItemErasure} and its contract, says the same
 * thing structurally instead of in a comment.
 */
class InMemoryCollectionRepositoryTest extends ItemErasureContractTest {

    private final InMemoryCollectionRepository store = new InMemoryCollectionRepository();

    @Override
    protected ItemErasure erasure() {
        return store;
    }

    @Override
    protected void givenSavedItem(String user, String collection, ItemRef ref) {
        store.add(user, collection, ref);
    }
}
