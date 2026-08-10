Feature: Removing a REFERENCE

  Taking a REFERENCE out of a COLLECTION is as idempotent as putting it in
  (workspace ADR 0006): removing something never saved is answered honestly,
  not with an error a retrying caller would trip over.

  Rule: A removed REFERENCE is gone from the COLLECTION

    Example:
      Given alice has saved meme 42 into "favourites"
      When alice removes meme 42 from "favourites"
      Then alice's "favourites" is empty

  Rule: Removing what was never there is told, not punished

    Example:
      When alice removes meme 42 from "favourites"
      Then the removal reports it was not there
