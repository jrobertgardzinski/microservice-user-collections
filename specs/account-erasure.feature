# The purge arrives over the broker and not over HTTP — the whole feature belongs to the
# application runner, and the HTTP runner filters it out by this tag. The wire itself — the
# envelope, the saga id, the confirmation that answers the orchestrator — is pact business.
# What this file describes is the PROMISE this service makes to the three others it shares
# a leaver with (ADR 0007).
@saga
Feature: What a leaver's saved lists are owed

  Anything this service holds of a person who is leaving has to go. What it holds are
  REFERENCES — somebody else's meme or comment, saved into a list of one's own — so
  emptying the lists destroys nothing that anyone else can see. It still has to be
  undoable: until somebody says the decision is final, every list has to be restorable
  exactly as it was.

  Being out of sight is not a courtesy here. Every other service holding that person's
  things is deciding at the same time, and any one of them may fail; a service that
  shredded the lists at once would have nothing to give back, and the leaver would get
  their account restored with their saved lists emptied.

  A decision acts only on what it reserved. Being told to finish something that set
  nothing aside must destroy nothing, and a request that names nobody is ignored rather
  than obeyed — this service never guesses whose things it is being asked for.

  Rule: Set aside, the lists are already empty as far as their owner can tell

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

  Rule: Nothing is destroyed until the decision is final

    Example:
      Given alice has saved meme 42 into "favourites"
      And alice has saved comment 7 into "watchlist"
      And alice's account is purged
      When the ORCHESTRATOR compensates the SAGA
      Then alice's "favourites" contains meme 42
      And alice's "watchlist" contains comment 7
      And no CONFIRMATION goes back to the ORCHESTRATOR

  Rule: Once it is final, nothing comes back

    Example:
      Given alice has saved meme 42 into "favourites"
      And alice's account is purged
      When the ORCHESTRATOR closes the SAGA
      Then alice's "favourites" is empty
      And a late compensation brings nothing back
      And no CONFIRMATION goes back to the ORCHESTRATOR

  Rule: Making it final destroys only what was set aside

    Example:
      Given alice has saved meme 42 into "favourites"
      When the ORCHESTRATOR closes the SAGA
      Then alice's "favourites" contains meme 42

  Rule: A request naming nobody is ignored

    Example:
      Given alice has saved meme 42 into "favourites"
      When a purge command arrives naming nobody
      Then no CONFIRMATION goes back to the ORCHESTRATOR
      And alice's "favourites" contains meme 42
