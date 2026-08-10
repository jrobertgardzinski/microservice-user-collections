Feature: Listing a COLLECTION

  A COLLECTION belongs to one USER and one name: alice's "favourites" is not
  bob's, and not alice's "watchlist". The listing shows the newest save first,
  and a GUEST gets no listing at all.

  Rule: The newest save is listed first

    Example:
      Given alice has saved meme 1 into "favourites"
      And alice has saved meme 2 into "favourites"
      Then alice's "favourites" lists meme 2 then meme 1

  Rule: COLLECTIONS are per person and per name

    Example:
      Given alice has saved meme 42 into "favourites"
      Then bob's "favourites" is empty
      And alice's "watchlist" is empty

  Rule: For a GUEST there are no COLLECTIONS

    # a refusal that only exists at the wire — the application entry point never sees it,
    # so this example runs through the HTTP runner alone
    @http
    Example:
      When a GUEST asks for "favourites"
      Then the collections stay closed to them
