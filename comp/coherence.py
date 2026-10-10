"""Is a query's postlist one discussion, or a bag of unrelated posts?

This is the gate that makes the whole selection work. Greedy set cover on
weight-per-token is structurally biased toward single common words — a superset
query always covers less at more cost — so no constraint on cost or rarity can
stop "oil" from beating "hormuz strait tanker". Only a purity test can, by
denying that "oil" covers anything at all.

Purity is measured as the mean pairwise Jaccard of the postlist's rare-term
sets. On this capture that statistic separates same-story pairs from random
pairs at AUC 0.993, so it is a well-founded stand-in for "these threads are the
same discussion".
"""
from __future__ import annotations

import numpy as np


class Coherence:
    def __init__(self, Mr: np.ndarray):
        """Mr: (n_terms, n_units) bool over *rare subject terms only*."""
        self.Mr = Mr.astype(np.float32)
        self.size = self.Mr.sum(0)

    def score(self, mask, exclude=(), max_sample=60, rng=None):
        idx = np.flatnonzero(mask)
        if idx.size < 2:
            return 1.0 if idx.size == 1 else 0.0
        if idx.size > max_sample:
            rng = rng or np.random.default_rng(0)
            idx = rng.choice(idx, max_sample, replace=False)
        X = self.Mr[:, idx]
        if exclude:
            X = X.copy()
            X[list(exclude), :] = 0.0
        inter = X.T @ X
        sz = np.diag(inter)
        union = sz[:, None] + sz[None, :] - inter
        with np.errstate(invalid="ignore", divide="ignore"):
            J = np.where(union > 0, inter / np.maximum(union, 1e-9), 0.0)
        iu = np.triu_indices(len(idx), 1)
        return float(J[iu].mean())
