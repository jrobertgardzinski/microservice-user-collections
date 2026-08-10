# the purge arrives over the broker, not over HTTP — the whole feature is the
# application runner's alone, and the HTTP runner filters it out by this tag
@saga
Feature: An account deletion empties the COLLECTIONS — carefully

  When a person leaves the portal, the deletion arrives as a SAGA command from the
  offboarding ORCHESTRATOR. The purge first sets the saved REFERENCES aside; only
  the ORCHESTRATOR's closure destroys them for good, and if the SAGA fails at
  another participant, a compensation brings everything back. Every answer travels
  back as a CONFIRMATION naming the SAGA — and a command that names nobody is
  ignored, not obeyed.

  Rule: The purge empties every COLLECTION at once and answers the ORCHESTRATOR

    Example:
      Given alice has saved meme 42 into "favourites"
      And alice has saved comment 7 into "watchlist"
      When alice's account is purged
      Then 2 REFERENCES were reserved
      And alice's "favourites" is empty
      And alice's "watchlist" is empty
      # The saga has two halves and only the refusal half was ever asserted: the suite proved that a
      # malformed command produces NO confirmation, never that a good one produces the right one.
      And a CONFIRMATION for that SAGA goes back to the ORCHESTRATOR

  Rule: A failure at another participant brings the saved list back

    Example:
      Given alice has saved meme 42 into "favourites"
      And alice has saved comment 7 into "watchlist"
      And alice's account is purged
      When the ORCHESTRATOR compensates the SAGA
      Then alice's "favourites" contains meme 42
      And alice's "watchlist" contains comment 7
      And no CONFIRMATION goes back to the ORCHESTRATOR

  Rule: The closure is the point of no return

    Example:
      Given alice has saved meme 42 into "favourites"
      And alice's account is purged
      When the ORCHESTRATOR closes the SAGA
      Then alice's "favourites" is empty
      And a late compensation brings nothing back
      And no CONFIRMATION goes back to the ORCHESTRATOR

  Rule: A closure for a SAGA that reserved nothing destroys nothing

    Example:
      Given alice has saved meme 42 into "favourites"
      When the ORCHESTRATOR closes the SAGA
      Then alice's "favourites" contains meme 42

  Rule: A purge naming nobody is ignored

    Example:
      Given alice has saved meme 42 into "favourites"
      When a purge command arrives naming nobody
      Then no CONFIRMATION goes back to the ORCHESTRATOR
      And alice's "favourites" contains meme 42
