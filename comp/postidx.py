"""Post-level inverted index: the postlist of a query, computed the way the
search engine computes it.

Bluesky matches a term conjunction against one post at a time. A thread's terms
are the union over its posts, so a query built from pooled thread vocabulary can
name a thread that no single post satisfies — and such an entry retrieves
nothing at all. Every candidate's postlist is therefore resolved here, against
posts, and only then mapped up to the discussion units it reaches.
"""
from __future__ import annotations

import collections

import numpy as np


class PostIndex:
    def __init__(self, post_terms, post2unit, n_units, tidx):
        self.n_units = n_units
        self.post2unit = np.asarray(post2unit, dtype=np.int32)
        post = collections.defaultdict(list)
        for p, ts in enumerate(post_terms):
            for t in ts:
                i = tidx.get(t)
                if i is not None:
                    post[i].append(p)
        self.post = {i: np.asarray(v, dtype=np.int32) for i, v in post.items()}

    def posts_for(self, items) -> np.ndarray:
        lists = [self.post.get(i) for i in items]
        if any(x is None for x in lists):
            return np.empty(0, dtype=np.int32)
        lists.sort(key=len)
        acc = lists[0]
        for x in lists[1:]:
            acc = np.intersect1d(acc, x, assume_unique=True)
            if acc.size == 0:
                break
        return acc

    def mask_for(self, items) -> np.ndarray:
        m = np.zeros(self.n_units, dtype=bool)
        p = self.posts_for(items)
        if p.size:
            m[self.post2unit[p]] = True
        return m
