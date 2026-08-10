Feature: Saving a REFERENCE

  A signed-in person saves an opaque REFERENCE — a meme, a comment — into a named
  COLLECTION. The service keeps the REFERENCE and nothing else: it never interprets
  what it points at. Saving is idempotent BY DEFAULT (workspace ADR 0006 — enforced
  by the generic IdempotentCommandsTest, not restated per scenario); the scenarios
  below pin the REPLY a caller can lean on.

  Rule: A saved REFERENCE lands in the COLLECTION

    Example:
      When alice saves meme 42 into "favourites"
      Then alice's "favourites" contains meme 42

  Rule: Saving twice is not an error — the caller is told it was already there

    Example:
      Given alice has saved meme 42 into "favourites"
      When alice saves meme 42 into "favourites"
      Then the save reports it was already there
      And alice's "favourites" contains meme 42 once

  Rule: A REFERENCE too long to be real is refused at the door

    # a refusal that only exists at the wire — the application entry point never sees it,
    # so this example runs through the HTTP runner alone
    @http
    Example:
      When alice saves a meme with a reference too long to be real into "favourites"
      Then the save is refused as nonsense
      And alice's "favourites" is empty
