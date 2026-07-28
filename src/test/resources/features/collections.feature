Feature: A user's collections of saved references
  A user saves opaque references — a meme, a comment — into named collections. The service keeps
  the refs and nothing else; it never interprets what they point at. Every command is idempotent
  BY DEFAULT (workspace ADR 0006 — enforced by the generic IdempotentCommandsTest, not restated
  per scenario); the scenarios below pin the REPLY contracts a caller can lean on. When the
  account is deleted every collection goes with it.

  Scenario: Saving a reference puts it in the collection
    When alice saves meme 42 into "favourites"
    Then alice's "favourites" contains meme 42

  Scenario: Saving twice tells the caller it was already there
    Given alice has saved meme 42 into "favourites"
    When alice saves meme 42 into "favourites"
    Then the save reports it was already there
    And alice's "favourites" contains meme 42 once

  Scenario: A collection lists its refs newest first
    Given alice has saved meme 1 into "favourites"
    And alice has saved meme 2 into "favourites"
    Then alice's "favourites" lists meme 2 then meme 1

  Scenario: Collections are per user and per name
    Given alice has saved meme 42 into "favourites"
    Then bob's "favourites" is empty
    And alice's "watchlist" is empty

  Scenario: Removing a reference takes it out
    Given alice has saved meme 42 into "favourites"
    When alice removes meme 42 from "favourites"
    Then alice's "favourites" is empty

  Scenario: Removing something not saved tells the caller it was not there
    When alice removes meme 42 from "favourites"
    Then the removal reports it was not there

  # @http scenarios pin refusals that only exist at the wire — the application entry point never
  # sees them, just as the HTTP entry point never sees the Kafka-borne @saga scenarios below.

  @http
  Scenario: A reference too long to be real is refused
    When alice saves a meme with a reference too long to be real into "favourites"
    Then the save is refused as nonsense
    And alice's "favourites" is empty

  @http
  Scenario: Without an identity there are no collections
    When somebody with no identity asks for "favourites"
    Then the collections stay closed to them

  @saga
  Scenario: Deleting the account purges every collection
    Given alice has saved meme 42 into "favourites"
    And alice has saved comment 7 into "watchlist"
    When alice's account is purged
    Then 2 references were removed
    And alice's "favourites" is empty
    And alice's "watchlist" is empty
    # The saga has two halves and only the refusal half was ever asserted: the suite proved that a
    # malformed command produces NO confirmation, never that a good one produces the right one.
    And a confirmation for that saga goes back to the orchestrator

  @saga
  Scenario: A purge naming nobody is ignored
    Given alice has saved meme 42 into "favourites"
    When a purge command arrives naming nobody
    Then no confirmation goes back to the orchestrator
    And alice's "favourites" contains meme 42
