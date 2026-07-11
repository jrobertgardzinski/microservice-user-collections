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

  @saga
  Scenario: Deleting the account purges every collection
    Given alice has saved meme 42 into "favourites"
    And alice has saved comment 7 into "watchlist"
    When alice's account is purged
    Then 2 references were removed
    And alice's "favourites" is empty
    And alice's "watchlist" is empty
