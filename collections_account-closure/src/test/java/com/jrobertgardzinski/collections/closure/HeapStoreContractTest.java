package com.jrobertgardzinski.collections.closure;

import com.jrobertgardzinski.collections.application.ItemErasure;
import com.jrobertgardzinski.collections.application.ItemErasureContract;
import com.jrobertgardzinski.collections.domain.ItemRef;

/** The heap this module's scenarios run on, held to what the real adapter promises. */
class HeapStoreContractTest extends ItemErasureContract {

    private final HeapStore store = new HeapStore();

    @Override
    protected ItemErasure erasure() {
        return store;
    }

    @Override
    protected void givenSavedItem(String user, String collection, ItemRef ref) {
        store.add(user, collection, ref);
    }
}
