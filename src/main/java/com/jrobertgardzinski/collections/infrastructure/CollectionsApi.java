package com.jrobertgardzinski.collections.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.domain.ItemRef;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.util.Optional;

/**
 * The HTTP boundary, mounted at {@code /collections}. Every route is a write to (or read of) the
 * caller's OWN data, so each first resolves the bearer token to a user through the {@link
 * SecurityGate}; no token, no access. The item is fully addressed by the path — no request body —
 * so a save is a plain idempotent PUT.
 *
 * <ul>
 *   <li>{@code GET  /collections/{collection}/items} — the refs, newest first (JSON array)</li>
 *   <li>{@code PUT  /collections/{collection}/items/{itemType}/{itemId}} — save (201 new, 200 already)</li>
 *   <li>{@code DELETE /collections/{collection}/items/{itemType}/{itemId}} — remove (204 gone, 404 absent)</li>
 * </ul>
 */
public class CollectionsApi implements HttpService {

    private final SaveItem saveItem;
    private final RemoveItem removeItem;
    private final ListItems listItems;
    private final SecurityGate gate;
    private final ObjectMapper mapper = new ObjectMapper();

    public CollectionsApi(SaveItem saveItem, RemoveItem removeItem, ListItems listItems, SecurityGate gate) {
        this.saveItem = saveItem;
        this.removeItem = removeItem;
        this.listItems = listItems;
        this.gate = gate;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/{collection}/items", this::list)
                .put("/{collection}/items/{itemType}/{itemId}", this::save)
                .delete("/{collection}/items/{itemType}/{itemId}", this::remove);
    }

    private void save(ServerRequest req, ServerResponse res) {
        Optional<String> user = authenticate(req);
        if (user.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401).send();
            return;
        }
        SaveItem.Status status = saveItem.execute(user.get(),
                req.path().pathParameters().get("collection"), itemOf(req));
        res.status(status == SaveItem.Status.SAVED ? Status.CREATED_201 : Status.OK_200).send();
    }

    private void remove(ServerRequest req, ServerResponse res) {
        Optional<String> user = authenticate(req);
        if (user.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401).send();
            return;
        }
        RemoveItem.Status status = removeItem.execute(user.get(),
                req.path().pathParameters().get("collection"), itemOf(req));
        res.status(status == RemoveItem.Status.REMOVED ? Status.NO_CONTENT_204 : Status.NOT_FOUND_404).send();
    }

    private void list(ServerRequest req, ServerResponse res) {
        Optional<String> user = authenticate(req);
        if (user.isEmpty()) {
            res.status(Status.UNAUTHORIZED_401).send();
            return;
        }
        ArrayNode array = mapper.createArrayNode();
        for (ItemRef item : listItems.execute(user.get(), req.path().pathParameters().get("collection"))) {
            array.addObject().put("itemType", item.itemType()).put("itemId", item.itemId());
        }
        try {
            res.header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(mapper.writeValueAsString(array));
        } catch (Exception unserialisable) {
            res.status(Status.INTERNAL_SERVER_ERROR_500).send();
        }
    }

    private static ItemRef itemOf(ServerRequest req) {
        return new ItemRef(req.path().pathParameters().get("itemType"),
                req.path().pathParameters().get("itemId"));
    }

    private Optional<String> authenticate(ServerRequest req) {
        return req.headers().first(HeaderNames.AUTHORIZATION)
                .filter(header -> header.startsWith("Bearer "))
                .map(header -> header.substring("Bearer ".length()))
                .flatMap(gate::userFor);
    }
}
