"""Render the selected discussions as the smallest thing an LLM can act on.

Measured decisions, all on the 6 h capture at ~150 entries:

*Rank order replaces counts.* Per-entry volume/engagement numbers cost ~2 tokens
each — more than all the labels put together — and mostly convey *relative*
prominence, which sorted order conveys for free. The header carries absolute
scale so the ordering can be calibrated.

*The label is the query.* Entries are keyqueries, so the same tokens serve as
topic name and retrieval key; nothing is spent making the index actionable.

*Terms are ordered most-discriminative-first.* Bluesky search is conjunctive, so
any prefix of an entry is a strictly broader query — a free recall/precision
dial, which matters because a conjunction cannot express the union a story
really is.
"""
from __future__ import annotations

from datetime import timedelta

from .tok import ntok
from .vocab import render

# Kept terse deliberately: at ~500 tokens of body, every 10 tokens of preamble is
# 2% of the artifact. But not terser — the header is the only thing that tells
# the reader these are queries rather than notes, and getting that wrong wastes
# far more than it saves.
# The bounds are emitted to the second, ready to paste: rounding a 20-minute
# window to "0h" (as an earlier version did) points the expansion search at an
# interval that excludes the very posts that formed the topic. `until` is the
# newest post + 1 s because searchPostsV2 treats it as exclusive.
#
# The fallback clause costs ~20 tokens and buys back two silent failures, each
# of which reads as "nobody discussed this" — the single most expensive way for
# this artifact to be wrong:
#   * search filters on INDEXING time, but the window is when posts reached the
#     timeline, so a reposted or resurfaced item can sit inside the window and
#     outside the bounds (measured at <4% of entries);
#   * `following=1` filters on the AUTHOR, while the home timeline also carries
#     posts that followed accounts reposted from people the reader does not
#     follow — those are in the index but outside a following-only search.
HEADER = (
    "BSKY-IDX since={since} until={until} ({span}) {posts}p/{authors}a; {n} topics, "
    "biggest first, {unit}-separated. Each IS an AND search query: "
    "search_posts(q, {scope}since, until). Fewer terms = wider. No hits -> "
    "{fallback}. get_thread a hit to read the whole conversation."
)

# The index is built from one source, but search is global. For the home
# timeline `following=1` is the closest approximation (reposts excepted, hence
# the fallback); for a feed or list nothing reproduces it, so the reader is
# told to expect hits from outside it.
SCOPE_FOLLOWING = "following=1, "
SCOPE_OTHER = ""
_FALLBACK = {
    SCOPE_FOLLOWING: "drop following (reposts reach past it), then since/until "
                     "(search uses indexing time)",
    SCOPE_OTHER: "drop since/until (search uses indexing time)",
}


def _bound(ts) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _span(seconds: float) -> str:
    if seconds < 120 * 60:
        return f"{max(1, round(seconds / 60))}m"
    h = seconds / 3600.0
    return f"{h:.1f}h" if h < 48 else f"{h / 24:.1f}d"


def encode(chosen, units, window, *, n_posts=0, n_authors=0, header=True,
           sep="; ", counts=False, residual=None, domains=None,
           top_authors=None, mutuals=None, scope=SCOPE_FOLLOWING,
           source_note=None):
    t0, t1 = window
    lines = []
    if header:
        since = t0.floor("s") if hasattr(t0, "floor") else t0.replace(microsecond=0)
        until = (t1.floor("s") if hasattr(t1, "floor") else t1.replace(microsecond=0)) \
            + timedelta(seconds=1)
        head = HEADER.format(since=_bound(since), until=_bound(until),
                             span=_span((t1 - t0).total_seconds()), posts=n_posts,
                             authors=n_authors, n=len(chosen), scope=scope,
                             fallback=_FALLBACK.get(scope, _FALLBACK[SCOPE_OTHER]),
                             unit="newline" if "\n" in sep else "';'")
        if source_note:
            head += " " + source_note
        lines.append(head)
    body = [" ".join(render(t) for t in c["q"]) for c in chosen]
    if counts:
        body = [f"{b} {c['posts']}" for b, c in zip(body, chosen)]
    lines += body
    if domains:
        lines.append("srcs: " + " ".join(domains))
    if top_authors:
        lines.append("loudest: " + " ".join(top_authors))
    if mutuals:
        lines.append("mutuals here: " + " ".join(mutuals))
    if residual:
        lines.append(residual)
    return sep.join(lines)


def residual_line(chosen, units):
    import numpy as np
    cov = np.zeros(len(units), dtype=bool)
    for c in chosen:
        cov |= c["mask"]
    rest = [u for i, u in enumerate(units) if not cov[i]]
    if not rest:
        return None
    return (f"+{len(rest)} unindexed threads (~{sum(u.n_posts for u in rest)} posts, "
            f"{sum(1 for u in rest if u.n_posts == 1)} single-post): "
            "no subject shared with anything else, mostly one-off chatter.")


def report(text):
    return dict(chars=len(text), tokens=ntok(text))
