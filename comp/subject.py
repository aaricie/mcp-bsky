"""Does a term name a subject, or is it conversational filler?

A pure information-theoretic score cannot tell them apart — "seems yeah" is
rarer on Bluesky (13.5 bits) than "saudi turkey pakistan" (10.5 bits) — so the
index needs a separate notion of subjecthood.

The trick used here is to let news headlines define the register. Link-card
titles in the firehose capture are ~68k real headlines: almost pure
subject-naming noun phrases. Comparing each term's rate in headlines against its
rate in ordinary post text, with Monroe et al.'s variance-adjusted log-odds
("Fightin' Words"), yields a domain-matched, data-driven stand-in for a POS
tagger — and it costs nothing at query time.

Measured separation on this corpus (higher = more subject-like):
    lol -6.9 · yeah -6.1 · basically -4.4 · think -4.3 · really -4.0
    drones +3.2 · pact +6.1 · tariffs +6.3 · unemployment +8.3 · hormuz +11.6
"""
from __future__ import annotations

import math
import pickle


class Subjecthood:
    def __init__(self, path="data/bg/subject.pkl"):
        with open(path, "rb") as f:
            d = pickle.load(f)
        self.z: dict[str, float] = d["z"]
        self.default: float = d["default"]

    def __call__(self, term: str) -> float:
        return self.z.get(term, self.default)


def fit(head_counts, body_counts, n_head, n_body, alpha0=500.0):
    """Monroe/Colaresi/Quinn log-odds ratio with an informative Dirichlet prior.

    The prior is the pooled corpus itself, so rare terms shrink toward zero
    instead of producing the spurious extremes a raw ratio gives them.
    """
    vocab = set(head_counts) | set(body_counts)
    total = sum(head_counts.values()) + sum(body_counts.values())
    z: dict[str, float] = {}
    for w in vocab:
        yi, yj = head_counts.get(w, 0), body_counts.get(w, 0)
        a0 = alpha0 * (yi + yj) / total                    # prior pseudo-count
        if a0 <= 0:
            continue
        num = (yi + a0) / (n_head + alpha0 - yi - a0)
        den = (yj + a0) / (n_body + alpha0 - yj - a0)
        d = math.log(num) - math.log(den)
        var = 1.0 / (yi + a0) + 1.0 / (yj + a0)
        z[w] = d / math.sqrt(var)
    return z
