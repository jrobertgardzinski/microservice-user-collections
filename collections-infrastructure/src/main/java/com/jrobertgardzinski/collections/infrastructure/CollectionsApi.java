package com.jrobertgardzinski.collections.infrastructure;

import com.jrobertgardzinski.collections.application.core.CollectionService;
import com.jrobertgardzinski.identity.UserId;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jrobertgardzinski.collections.domain.core.ItemRef;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.util.List;
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
 *
 * <p>Every refusal carries a body, and always the same shape: {@code {"status":"CODE"}}, the one
 * memes and comments answer with. A bare status told a caller a request was refused and nothing
 * about which of three different things was wrong with it — an over-long collection, item type and
 * item id all answered 400 with zero bytes, and a client parsing the answer got a JSON error
 * instead of a reason.
 */
public class CollectionsApi implements HttpService {

    private final CollectionService collections;
    private final SecurityGate gate;
    private final ObjectMapper mapper = new ObjectMapper();

    public CollectionsApi(CollectionService collections, SecurityGate gate) {
        this.collections = collections;
        this.gate = gate;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get("/{collection}/items", this::list)
                .put("/{collection}/items/{itemType}/{itemId}", this::save)
                .delete("/{collection}/items/{itemType}/{itemId}", this::remove);
    }

    private void save(ServerRequest req, ServerResponse res) {
        Optional<Caller> caller = authenticate(req);
        if (caller.isEmpty()) {
            refuse(res, Status.UNAUTHORIZED_401, "UNAUTHENTICATED");
            return;
        }
        switch (collections.save(caller.get().userId(), param(req, "collection"), param(req, "itemType"),
                param(req, "itemId"))) {
            case CollectionService.Saving.Saved saved -> res.status(Status.CREATED_201).send();
            case CollectionService.Saving.AlreadySaved already -> res.status(Status.OK_200).send();
            case CollectionService.Saving.TooLong tooLong -> refuse(res, Status.BAD_REQUEST_400, tooLong.code());
        }
    }

    private void remove(ServerRequest req, ServerResponse res) {
        Optional<UserId> user = authenticate(req).map(Caller::userId);
        if (user.isEmpty()) {
            refuse(res, Status.UNAUTHORIZED_401, "UNAUTHENTICATED");
            return;
        }
        switch (collections.remove(user.get(), param(req, "collection"), param(req, "itemType"),
                param(req, "itemId"))) {
            case CollectionService.Removal.Removed removed -> res.status(Status.NO_CONTENT_204).send();
            case CollectionService.Removal.NotSaved notSaved -> refuse(res, Status.NOT_FOUND_404, "NOT_SAVED");
            case CollectionService.Removal.TooLong tooLong -> refuse(res, Status.BAD_REQUEST_400, tooLong.code());
        }
    }

    private void list(ServerRequest req, ServerResponse res) {
        Optional<UserId> user = authenticate(req).map(Caller::userId);
        if (user.isEmpty()) {
            refuse(res, Status.UNAUTHORIZED_401, "UNAUTHENTICATED");
            return;
        }
        List<ItemRef> items;
        switch (collections.list(user.get(), param(req, "collection"))) {
            case CollectionService.Listing.TooLong tooLong -> {
                refuse(res, Status.BAD_REQUEST_400, tooLong.code());
                return;
            }
            case CollectionService.Listing.Listed listed -> items = listed.items();
        }
        ArrayNode array = mapper.createArrayNode();
        for (ItemRef item : items) {
            array.addObject().put("itemType", item.itemType()).put("itemId", item.itemId());
        }
        try {
            res.header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(mapper.writeValueAsString(array));
        } catch (Exception unserialisable) {
            refuse(res, Status.INTERNAL_SERVER_ERROR_500, "LIST_UNREADABLE");
        }
    }

    /**
     * The one error shape, spelled by hand: every code below is a literal of this class, so there
     * is nothing here a caller could get into the body.
     */
    private static void refuse(ServerResponse res, Status status, String code) {
        res.status(status)
                .header(HeaderNames.CONTENT_TYPE, "application/json")
                .send("{\"status\":\"" + code + "\"}");
    }


    /**
     * Which segment of the item path is wider than its column, as the code the caller is given —
     * null when they all fit. Three causes that used to share one bodiless 400, so a client could
     * tell a mistyped collection from a pasted URL only by measuring the path itself.
     */


    private static String param(ServerRequest req, String name) {
        return req.path().pathParameters().get(name);
    }

    private Optional<Caller> authenticate(ServerRequest req) {
        return req.headers().first(HeaderNames.AUTHORIZATION)
                .filter(header -> header.startsWith("Bearer "))
                .map(header -> header.substring("Bearer ".length()))
                .flatMap(gate::callerFor);
    }
}
