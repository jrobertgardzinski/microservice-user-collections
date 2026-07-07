package com.jrobertgardzinski.collections.domain;

/**
 * An opaque reference to a saved thing: its type (e.g. {@code "meme"}, {@code "comment"}) and its
 * id in that source. This service never interprets either — no invariant is shared with the source
 * domain, which is exactly what lets any new kind of collectable join without a code change here.
 * Per the workspace ADR the domain does not guard null; the boundary keeps null out.
 */
public record ItemRef(String itemType, String itemId) {
}
