"""A local stand-in for Bluesky's search index, built from a firehose capture.

The keyquery definition needs a *reference search engine* to answer "how many
documents does this query retrieve?". Bluesky's own API can answer that, but not
for the ~10^5 candidate queries the miner generates. A 247k-post firehose sample
answers it offline and exactly: because search is conjunctive, the hit count of a
term conjunction is the size of the intersection of its postlists.

The resulting number is what makes the index self-aware about specificity — it
is the difference between "pact" (0.06% of posts) and "yeah" (0.5%), and between
a query that names an event and one that names nothing.
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

# Resolved against the package, not the process CWD: the MCP server imports
# `comp` as a library and its working directory is not ours to assume.
BG_DIR = Path(__file__).resolve().parent.parent / "data" / "bg"


class Background:
    def __init__(self, path=None):
        path = path or BG_DIR / "firehose_idx.pkl"
        with open(path, "rb") as f:
            d = pickle.load(f)
        self.N: int = d["N"]
        self.post: dict[str, np.ndarray] = d["post"]     # term -> sorted doc ids
        self.df: dict[str, int] = {t: len(v) for t, v in self.post.items()}

    def hits(self, terms) -> int:
        """Exact number of background docs matching the conjunction."""
        lists = []
        for t in terms:
            p = self.post.get(t)
            if p is None:
                return 0                                  # unseen -> assume rare
            lists.append(p)
        lists.sort(key=len)
        acc = lists[0]
        for p in lists[1:]:
            acc = np.intersect1d(acc, p, assume_unique=True)
            if acc.size == 0:
                break
        return int(acc.size)

    def bits(self, terms, floor=0.5) -> float:
        """Information content of the query: how much of the world it rules out.

        `floor` applies Laplace-style smoothing so a query unseen in the sample
        does not claim infinite specificity.
        """
        h = max(self.hits(terms), floor)
        return float(-np.log2(h / self.N))

    def rate(self, terms) -> float:
        return self.hits(terms) / self.N
