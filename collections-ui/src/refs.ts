/**
 * Resolving a saved reference — and the read-repair rule that follows from it.
 *
 * A collection row is an OPAQUE pointer: `(itemType, itemId)`, with no invariant shared with the
 * service that owns the thing pointed at. So the only way to know whether an entry still means
 * anything is to ask that service. What the UI does with the answer is where this file earns its
 * existence, because the obvious behaviour is the wrong one.
 *
 * ## Why a 404 must NEVER delete the favourite
 *
 * The tempting design is "the meme is gone, so tidy the row away". The audit of the live stack
 * (AUDYT-2026-07-26, finding S12/#4) is the reason it is forbidden here: **26 of 91 memes answer
 * 404 while their row exists and their author is a real account.** The gallery's existence checks
 * read the image blob to answer "does this exist?", so a meme whose bytes moved store answers 404
 * on every read path — `/meta`, `/thumbnail`, the image itself, even DELETE — while being, by any
 * honest definition, still there. An auto-repairing UI would have walked that list and silently
 * deleted a quarter of everybody's favourites, pointing at memes that are coming back the moment
 * the store is fixed. Deletion is not reversible; a wrong tile is.
 *
 * So the rule is: **the UI classifies, the human decides.** A reference that cannot be resolved is
 * rendered as unavailable, with an explicit button that removes it — one entry, one deliberate
 * click, no batch, no background sweep.
 *
 * ## Why a 5xx or a timeout is a different answer entirely
 *
 * `gone` is a claim about the WORLD ("the gallery says it does not have this"). `unknown` is a
 * claim about US ("we could not find out"). Collapsing the two is how a five-minute deployment of
 * the meme service turns into every tile in the portal offering to delete itself. Everything that
 * is not an explicit "not found" — 500, 502, 429, a timeout, a blocked cross-origin request, an
 * offline laptop — resolves to `unknown`, which renders as an error state and offers no repair at
 * all.
 *
 * The real cleanup is not this UI's job anyway: the cascade consumer (CascadeConsumer) removes
 * references when the portal actually announces a deletion. This is the honest rendering of
 * whatever that cascade missed.
 */

/** What we know about a saved reference right now. */
export type Resolution =
  /** the question is in flight */
  | { state: 'checking' }
  /** the owning service answered "here it is" */
  | { state: 'live' }
  /** the owning service answered, explicitly, "I do not have this" */
  | { state: 'gone' }
  /** we could not find out — NEVER a reason to offer, let alone perform, a deletion */
  | { state: 'unknown'; why: string };

/**
 * How long we wait for an answer before calling it `unknown`. Short on purpose: a slow gallery
 * must not leave a wall of tiles spinning, and "we could not check" is a perfectly useful thing
 * to tell someone.
 */
export const RESOLVE_TIMEOUT_MS = 5_000;

/**
 * The whole rule, as a pure function of an HTTP status — so it can be read, tested and argued
 * about without a browser. Only an explicit "not found" is allowed to mean `gone`.
 */
export function resolutionForStatus(status: number): Resolution {
  if (status >= 200 && status < 300) {
    return { state: 'live' };
  }
  if (status === 404 || status === 410) {
    // 410 Gone is the stronger form of the same statement; both are the owning service telling
    // us about the WORLD rather than about itself
    return { state: 'gone' };
  }
  if (status === 401 || status === 403) {
    // "you may not look" is not "it is not there" — a tightened gallery policy must never read
    // as a portal-wide deletion offer
    return { state: 'unknown', why: `the gallery refused the check (${status})` };
  }
  return { state: 'unknown', why: `the gallery answered ${status}` };
}

/**
 * Ask the gallery whether a meme is still there.
 *
 * The thumbnail, not `/meta`. `/meta` used to hand back the author's raw e-mail address (the
 * audit's sharpest finding — a public endpoint leaking PII to anonymous callers) and the gallery
 * has since masked it, so this is no longer a leak we are dodging. It stays the thumbnail anyway,
 * for the reason that outlives the fix: a UI that needs a yes/no should ask for a yes/no, not pull
 * a stranger's metadata into the browser and throw all of it away. Only the STATUS is read; the
 * bytes are discarded.
 *
 * `base` is empty by default, i.e. same origin, which is what the nginx in front of this bundle is
 * configured for (nginx.conf.template) — a cross-origin request to the gallery would be blocked by
 * the browser and arrive here as an indistinguishable network failure, and this file's entire
 * point is that a failure to check must not look like an answer.
 */
export async function resolveMeme(
  base: string,
  itemId: string,
  doFetch: typeof fetch = fetch,
): Promise<Resolution> {
  try {
    const response = await doFetch(
      `${base}/memes/${encodeURIComponent(itemId)}/thumbnail`,
      { signal: AbortSignal.timeout(RESOLVE_TIMEOUT_MS) },
    );
    return resolutionForStatus(response.status);
  } catch {
    // a timeout, a dropped connection, a CORS refusal, an offline browser — we learned nothing
    return { state: 'unknown', why: 'the gallery did not answer' };
  }
}

/**
 * Which item types this UI can resolve at all. A type with no resolver is rendered plainly: we
 * have no opinion about it, and "no opinion" must never render as a problem — inventing a
 * `gone` for a type nobody taught us to check would be the auto-delete bug wearing a hat.
 */
export function isResolvable(itemType: string): boolean {
  return itemType === 'meme';
}

/** The stable identity of a reference — the key for both React and the resolution map. */
export function refKey(ref: { itemType: string; itemId: string }): string {
  return `${ref.itemType}/${ref.itemId}`;
}
