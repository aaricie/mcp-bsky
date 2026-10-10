"""Candidate vocabulary: which terms may appear in an index entry at all.

Two jobs. Keep the term matrix small enough that itemset mining is instant, and
keep conversational filler out of the labels — an entry reading "yeah actually"
is worthless however cheap it is.

Filler is identified with the background priors rather than a hand-written
stoplist, so it transfers across languages and across whatever the timeline
happens to talk about.
"""
from __future__ import annotations

import functools
import math
import pickle

import numpy as np

from .tok import ntok

try:
    from wordfreq import zipf_frequency
    _HAVE_WF = True
except Exception:                                            # pragma: no cover
    _HAVE_WF = False

# Languages this timeline actually speaks, checked in descending order of volume.
_LANGS = ("en", "fr", "es", "de", "pt", "it", "nl", "ru", "uk", "pl", "ja", "ko", "zh")


@functools.lru_cache(maxsize=None)
def _freq_dict(lang: str) -> dict:
    """Whole-language frequency table, fetched once.

    `zipf_frequency` re-tokenises on every call; at ~40k vocabulary terms times
    13 languages that was 30 s of a 37 s run. The same numbers come out of one
    dict lookup per language.
    """
    if not _HAVE_WF:
        return {}
    try:
        from wordfreq import get_frequency_dict
        return get_frequency_dict(lang, wordlist="best")
    except Exception:
        return {}


@functools.lru_cache(maxsize=400_000)
def max_zipf(t: str) -> float:
    """How common the word is in the commonest language that has it.

    The third prior, and the one that covers what the other two cannot. The
    headline z-score is computed on a firehose sample that is overwhelmingly
    English, so a French function word has too little evidence to score
    negative and is shrunk to ~0 by the Monroe prior — which let "oui", "quel",
    "pere" and "parti" through as topics. Corpus-independent word frequency
    sees them immediately.
    """
    if not _HAVE_WF:
        return 0.0
    best = 0.0
    for lg in _LANGS:
        f = _freq_dict(lg).get(t)
        if f:
            z = math.log10(f) + 9.0
            if z > best:
                best = z
                if best >= 6.0:
                    break                    # already unambiguously common
    return best


@functools.lru_cache(maxsize=400_000)
def term_cost(t: str) -> int:
    """Marginal token cost of appending this term to a query."""
    return max(1, ntok(" " + t))


@functools.lru_cache(maxsize=400_000)
def render(t: str) -> str:
    """Cheapest surface form. Bluesky search is case-insensitive, so a term is
    free to be written whichever way tokenises shorter — 'Hormuz' is often one
    token where 'hormuz' is two."""
    cap = t[:1].upper() + t[1:]
    return cap if ntok(" " + cap) < ntok(" " + t) else t


class Priors:
    def __init__(self, path=None):
        from .bg import BG_DIR
        path = path or BG_DIR / "subject.pkl"
        with open(path, "rb") as f:
            d = pickle.load(f)
        self.z = d["z"]
        self.caprate = d["caprate"]
        self.default_cap = d["default_cap"]

    def headline_z(self, t):
        v = self.z.get(t)
        return 0.0 if v is None or v != v else v

    def cap(self, t, local=None):
        v = self.caprate.get(t)
        if v is None:
            return self.default_cap if local is None else local
        return v

    def is_filler(self, t, local_cap=None, z_floor=-6.0, cap_floor=0.25,
                  bg_rate=None, common=0.009, strong_z=2.0, strong_cap=0.60,
                  common_zipf=4.6):
        """Is this word incapable of naming a subject?

        Three priors, consulted in order of how much they know:

        1. strong headline evidence -> subject word, keep ("trump", "pirro").
        2. very common on Bluesky    -> filler, whatever its case. English
           capitalises "I" everywhere, so "i'm" and "i've" score caprate ~1.0
           and posed as proper nouns; one measured label read "i'm trump".
        3. strong proper-noun rate   -> a name, keep ("Delta", "Uber", "Odyssey").
        4. common in some language   -> filler. The catch-all for terms the
           headline prior has too little evidence about, which on an
           English-dominated background corpus means every non-English word
           ("oui", "quel", "pere", "les").
        5. clearly anti-headline and not a name.

        The order carries the whole rule: 1 before 2 or common topic words are
        lost, 2 before 3 or pronouns pose as names, 3 before 4 or names that are
        also ordinary words are lost.
        """
        z, cap = self.headline_z(t), self.cap(t, local_cap)
        if z >= strong_z:
            return False
        if bg_rate is not None and bg_rate >= common:
            return True
        if cap >= strong_cap:
            return False
        if max_zipf(t) >= common_zipf:
            return True
        return z < z_floor and cap < cap_floor

    def subjecthood(self, t, local_cap=None):
        """Single comparable score, used only for ordering candidates."""
        return max(self.headline_z(t) / 4.0, (self.cap(t, local_cap) - 0.12) / 0.22)


def local_caprates(df, threads):
    """Capitalisation rates measured on the timeline itself, so terms the
    background corpus has never seen ('Klippert', 'Montembeault') are still
    recognised as names."""
    from .data import cap_scan
    import collections
    cap, low = collections.Counter(), collections.Counter()
    for c in ("text", "ext_title", "ext_desc", "alt_text"):
        for v in df[c]:
            cap_scan(v, cap, low)
    out = {}
    for w in set(cap) | set(low):
        c, l = cap.get(w, 0), low.get(w, 0)
        out[w] = (c + 3 * 0.12) / (c + l + 3)
    return out


def build(threads, priors, local_cap, *, min_df=2, max_df_frac=0.15,
          min_len=2, max_cost=5, bgidx=None, min_bits=0.0):
    """Term -> boolean thread-membership matrix, filtered to plausible query terms."""
    n = len(threads)
    df: dict[str, int] = {}
    for th in threads:
        for t in th.terms:
            df[t] = df.get(t, 0) + 1

    max_df = max(min_df, int(max_df_frac * n))
    keep = []
    for t, d in df.items():
        if d < min_df or d > max_df or len(t) < min_len:
            continue
        if t.isdigit():
            continue
        if term_cost(t) > max_cost:
            continue
        rate = (bgidx.df.get(t, 0) / bgidx.N) if bgidx is not None else None
        if priors.is_filler(t, local_cap.get(t), bg_rate=rate):
            continue
        if bgidx is not None and min_bits > 0 and bgidx.bits([t]) < min_bits:
            continue
        keep.append(t)
    keep.sort()

    tidx = {t: i for i, t in enumerate(keep)}
    M = np.zeros((len(keep), n), dtype=bool)
    for j, th in enumerate(threads):
        for t in th.terms:
            i = tidx.get(t)
            if i is not None:
                M[i, j] = True
    return keep, M, tidx
