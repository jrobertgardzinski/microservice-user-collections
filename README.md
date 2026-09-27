# microservice-user-collections

A user's saved references — memes, comments — kept in named collections ("favourites",
"watchlist"). The references are **opaque**: an `(itemType, itemId)` pair the service stores and
returns but never interprets, which is what keeps it out of every other service's business.
Helidon SE (imperative, blocking, scaling on Loom — a deliberately different flavour from the
Boot/Micronaut/Quarkus siblings), the estate's four layers as four Maven modules
(`collections-domain` / `collections-config` / `collections-application` /
`collections-infrastructure`), Postgres + Flyway (H2 in PostgreSQL mode for dev and tests).

Beside them sits `collections_account-closure`: not a layer, but this service's part in ONE
cross-service process — what happens to a person's saved references when their account closes.
The underscore says so. `collections-<x>` is a layer; `collections_<x>` is a participation, named
after the library the participants speak through, and `memes_account-closure` and
`comments_account-closure` are the other ends of the same conversation.

The layers are modules rather than packages because a package boundary is a convention and a
module boundary is a classpath: `collections-application` compiles without Helidon, JDBC or Kafka
on it, and `collections-infrastructure` is the only module that produces something you can run —
`collections-infrastructure/target/collections-infrastructure.jar` plus the `libs/` its manifest
points at.

## Who it talks to

- **microservice-security** — every route requires a signed-in user. The gate is **offline**:
  the access token's EdDSA signature is verified against security's `/.well-known/jwks.json`
  (keys cached, an unknown `kid` refetches once); no per-request call to security, at the price
  of revocation blindness until the token's `exp`.
- **microservice-memes / microservice-comments** — the deletion cascade, consumed: `MEME_DELETED`
  on `memes-events` and `COMMENTS_DELETED` on `comments-events` drop every user's reference to
  the deleted thing (and *only* that thing — the item type is half the key, because the two
  sources mint ids independently). One consumer thread serves both topics, in its own consumer
  group. The policy is deliberately **best-effort**: a store failure is retried a bounded number
  of times, then the event is abandoned rather than wedging the partition — a dead reference
  survives (the UI renders it as unavailable), and the next deletion still cascades. The topic
  names and event shapes are pinned by message pacts (`pacts/`, verified by the producers) and by
  `CascadeTopicNamesTest` on this side.
- **microservice-offboarding** — the account-deletion saga, this service as participant
  (ADR 0007, two phases): `PURGE_USER_CONTENT` on `content-commands` **marks** the leaver's rows
  (`PENDING_ERASURE` — hidden from every read, deleted from nothing) and confirms with
  `USER_CONTENT_PURGED` on `usercollections-events`; `ERASE_USER_CONTENT` deletes exactly the
  rows the mark reserved; `RESTORE_USER_CONTENT` compensates, putting the lists back as they
  were. Reads go through the `active_collection_items` view — a build-time guard
  (`ItemReadFilterTest`) fails the suite if any other query names the base table — and the
  `collections_erasure_backlog` gauge alarms when a mark stays unresolved longer than any saga
  can last, because that failure is otherwise silent by construction.
  The closure destroys what the mark reserved and nothing else, and that leaves one hole this
  service cannot close alone: the gate here is **offline**, so the leaver's own access token keeps
  being accepted for up to its `exp` (an hour by default) after the deletion starts, and a save in
  that window lands ACTIVE — outside the mark, so outside the closure. Such a row is not deleted
  (a wholesale delete on a redelivered closure would destroy references the closure never marked,
  and since the key is an id the estate never reassigns, nobody else's list is at risk either), it
  is **counted**: `collections_erasure_residue_total` plus a WARN naming the saga, so a reference
  left standing after an erasure can be found and removed by hand. Closing it properly needs a revocation signal from security (or an online check per
  request) — the reservation itself is safe meanwhile: a reserved row is invisible in every listing
  and a `DELETE` from that same stale tab reports "not there" rather than destroying it.
- **microservice-security, the other direction** — a member's address can move, and their lists do
  not notice. Every row is keyed by the member's id (`collection_items.user_id`, from the JWT's
  `sub`; `uq_collection_item` is keyed by it too), so a rename changes nothing here and there is no
  third Kafka loop any more — the consumer that listened for `EMAIL_CHANGED` and the rekey it drove
  are gone with the cutover (workspace ADR 0008). This service keeps no address at all: it stores
  references, and the only name it would ever show comes from security at read time. A build-time
  guard (`RetiredAddressKeyTest`) fails the suite if an address-shaped column or the retired
  machinery comes back. The confirmation sent on `usercollections-events` still carries `reserved`
  — how many references the mark actually took out of the member's lists — and a zero still raises
  `collections_saga_purge_reserved_nothing_total` instead of reading as a successful erasure.
- **collections-ui** (port 8093) — the favourites UI on its own origin, so the CORS conversation
  is real: an allowlisted origin gets its echo and a fully-answered preflight, a foreign one gets
  no CORS headers at all.

## Contract

```
GET    /collections/{collection}/items                     -> 200 [ { itemType, itemId } ]  (newest first)
PUT    /collections/{collection}/items/{itemType}/{itemId} -> 201 saved | 200 already there | 400 | 401
DELETE /collections/{collection}/items/{itemType}/{itemId} -> 204 removed | 404 not saved | 400 | 401
```

Every command is idempotent by default (workspace ADR 0006, enforced generically by
`IdempotentCommandsTest`); the statuses above are the *reply* contract a caller can lean on.
Path segments wider than the schema's columns are refused with 400 at the edge, never a
`SQLException` from below.

Every refusal carries the estate's one error shape — `{"status":"CODE"}`, as memes and comments
answer — so the three different 400s can be told apart: `UNAUTHENTICATED` (401),
`COLLECTION_TOO_LONG` / `ITEM_TYPE_TOO_LONG` / `ITEM_ID_TOO_LONG` (400), `NOT_SAVED` (404).

Two probes, two questions: `/health` (readiness) turns 503 when the saga consumer stops
finishing records or the broker stops answering the round-trip probe; `/alive` (liveness) only
watches that the loop thread still schedules, so a database or broker outage does not get a pod
restarted for nothing. `/metrics` exposes, among others, the dropped-records counter, the
erasure backlog gauge and the erasure residue counter.

## Run & test

```bash
../mvnw -f pom.xml test    # unit + black-box HTTP on a real WebServer, JDBC adapters on H2
```

The behaviour contract is the Gherkin specs in [`specs/`](./specs) — one file per use case
(save, remove, list, account erasure), each scenario driven through **two** entry points: the
application layer and real HTTP (the spec-first pattern from microservice-security). Message
pacts in [`pacts/`](./pacts) are verified by the producing services.

In the compose stack: port 8092, own Postgres (`collections-postgres`).

## Documentation

- [`specs/`](./specs) — the executable specifications: Gherkin, one file per use case, run through both entry points by every build.
- [`Documentation.md`](./Documentation.md) — the epic → feature → story tree, generated from the test suite's Allure reports; regenerate with `../create-documentation.sh` after `./mvnw clean test`.
