"""Measure whether the index actually works.

Three questions, three metrics, none of them self-reported by the selector:

  coverage        what share of the timeline's discussion mass is reachable from
                  at least one entry (the compression side of the trade).
  story recall    of the news stories demonstrably present in the window —
                  defined by shared article URLs, so the reference is data, not
                  judgement — how many does some entry retrieve? This is the
                  metric that would catch an index full of well-formed labels
                  that happen to name nothing that was discussed.
  live retrieval  do the entries work against the real Bluesky search, not the
                  offline simulation of it.
"""
from __future__ import annotations

import collections

import numpy as np

from . import data, units


def coverage(chosen, uu):
    n = len(uu)
    cov = np.zeros(n, dtype=bool)
    for c in chosen:
        cov |= c["mask"]
    posts = np.array([u.n_posts for u in uu])
    eng = np.array([u.likes + 2 * u.reposts for u in uu])
    return dict(
        units=float(cov.mean()),
        posts=float(posts[cov].sum() / max(posts.sum(), 1)),
        engagement=float(eng[cov].sum() / max(eng.sum(), 1)),
        n_covered=int(cov.sum()),
    )


def reference_stories(df, threads, uu, *, min_threads=2):
    """Stories that provably happened: >=2 units carrying the same article URL.

    Only link-carrying discussions are visible this way, which is a real
    limitation and is reported rather than hidden — but it is a reference the
    selector has no access to and cannot have fitted.
    """
    t2u = {}
    for i, u in enumerate(uu):
        for t in u.tids:
            t2u[t] = i
    by_url = collections.defaultdict(set)
    for th in threads:
        for x in th.urls:
            c = units.canon_url(x)
            if c:
                by_url[c].add(t2u[th.tid])
    out = []
    for c, us in by_url.items():
        if len(us) >= min_threads:
            out.append(sorted(us))
    return out


def story_recall(chosen, stories, uu, *, need=0.5):
    """A story counts as recalled if one entry retrieves at least `need` of it."""
    hit, detail = 0, []
    for s in stories:
        best, who = 0.0, None
        for c in chosen:
            f = c["mask"][s].mean()
            if f > best:
                best, who = f, c
        detail.append((s, best, who))
        if best >= need:
            hit += 1
    return hit / max(len(stories), 1), detail


def headline_reference(df, uu, threads, *, top=40):
    """The most-shared article headlines, as a human-readable check list."""
    t2u = {}
    for i, u in enumerate(uu):
        for t in u.tids:
            t2u[t] = i
    seen = collections.defaultdict(lambda: [0, "", set()])
    for _, r in df.iterrows():
        if r.ext_title and r.ext_uri:
            c = units.canon_url(r.ext_uri) or r.ext_uri
            seen[c][0] += 1 + int(r.n_likes or 0) // 50
            seen[c][1] = str(r.ext_title)
    return sorted(seen.values(), key=lambda x: -x[0])[:top]
