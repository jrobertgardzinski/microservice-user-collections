package com.jrobertgardzinski.collections.httpsteps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.collections.application.ListItems;
import com.jrobertgardzinski.collections.application.RemoveItem;
import com.jrobertgardzinski.collections.application.SaveItem;
import com.jrobertgardzinski.collections.infrastructure.CollectionsApi;
import com.jrobertgardzinski.collections.infrastructure.InMemoryCollectionStore;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.helidon.webserver.WebServer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The same Gherkin, driven black-box over HTTP against a real Helidon WebServer (in-memory store,
 * fake gate). A username in a sentence becomes a {@code Bearer} token; the response status and the
 * listing JSON are the assertions. The HTTP entry point of the spec-first pair.
 */
public class HttpCollectionsSteps {

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private WebServer server;
    private String baseUrl;
    private int lastStatus;

    @Before
    public void startServer() {
        InMemoryCollectionStore store = new InMemoryCollectionStore();
        CollectionsApi api = new CollectionsApi(
                new SaveItem(store), new RemoveItem(store), new ListItems(store), new FakeGate());
        server = WebServer.builder()
                .port(0)   // a free port
                .routing(routing -> routing.register("/collections", api))
                .build()
                .start();
        baseUrl = "http://localhost:" + server.port();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @When("^(\\w+) saves (\\w+) (\\d+) into \"([^\"]+)\"$")
    @Given("^(\\w+) has saved (\\w+) (\\d+) into \"([^\"]+)\"$")
    public void saves(String user, String type, String id, String collection) {
        lastStatus = send(user, "PUT", itemUri(collection, type, id));
    }

    @When("^(\\w+) removes (\\w+) (\\d+) from \"([^\"]+)\"$")
    public void removes(String user, String type, String id, String collection) {
        lastStatus = send(user, "DELETE", itemUri(collection, type, id));
    }

    @Then("^the save reports it was already there$")
    public void saveWasIdempotent() {
        assertEquals(200, lastStatus, "a repeated save answers 200 OK, not 201 Created");
    }

    @Then("^the removal reports it was not there$")
    public void removalWasIdempotent() {
        assertEquals(404, lastStatus, "removing something absent answers 404");
    }

    @Then("^(\\w+)'s \"([^\"]+)\" contains (\\w+) (\\d+)$")
    public void contains(String user, String collection, String type, String id) {
        assertTrue(listing(user, collection).stream().anyMatch(ref(type, id)));
    }

    @Then("^(\\w+)'s \"([^\"]+)\" contains (\\w+) (\\d+) once$")
    public void containsOnce(String user, String collection, String type, String id) {
        assertEquals(1, listing(user, collection).stream().filter(ref(type, id)).count());
    }

    @Then("^(\\w+)'s \"([^\"]+)\" lists (\\w+) (\\d+) then (\\w+) (\\d+)$")
    public void listsInOrder(String user, String collection,
                             String firstType, String firstId, String secondType, String secondId) {
        var items = listing(user, collection);
        assertTrue(ref(firstType, firstId).test(items.get(0)));
        assertTrue(ref(secondType, secondId).test(items.get(1)));
    }

    @Then("^(\\w+)'s \"([^\"]+)\" is empty$")
    public void isEmpty(String user, String collection) {
        assertTrue(listing(user, collection).isEmpty());
    }

    // --- HTTP plumbing ------------------------------------------------------

    private String itemUri(String collection, String type, String id) {
        return baseUrl + "/collections/" + collection + "/items/" + type + "/" + id;
    }

    private int send(String user, String method, String uri) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(uri))
                    .header("Authorization", "Bearer " + user)
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build();
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (Exception e) {
            throw new IllegalStateException(method + " " + uri + " failed", e);
        }
    }

    private java.util.List<JsonNode> listing(String user, String collection) {
        try {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(baseUrl + "/collections/" + collection + "/items"))
                    .header("Authorization", "Bearer " + user).GET().build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), "listing should answer 200");
            java.util.List<JsonNode> items = new java.util.ArrayList<>();
            mapper.readTree(response.body()).forEach(items::add);
            return items;
        } catch (Exception e) {
            throw new IllegalStateException("GET listing failed", e);
        }
    }

    private static java.util.function.Predicate<JsonNode> ref(String type, String id) {
        return node -> type.equals(node.path("itemType").asText()) && id.equals(node.path("itemId").asText());
    }
}
