"""Merge threads into discussion units using links that are known, not inferred.

Three edges are free and exact in the capture:
  * reply    — already folded in by data.load (thread = reply_root)
  * quote    — a quote post is about what it quotes
  * article  — two posts carrying the same canonical URL are the same story

This matters because the two edge types have opposite similarity profiles.
Measured on this capture (99 positive pairs each, 4000 negatives):

                       shared-article    quote edges
    rare-term Jaccard      AUC 0.993       AUC 0.783
    gte-modernbert         AUC 0.814       AUC 0.869

News stories are joined by vocabulary; commentary is not. Rather than blend the
two similarities — which costs 0.02 AUC on the population keyqueries actually
serve — the structural edges are consumed directly and the remaining inference
is left purely lexical, which is also what a keyword query can retrieve.
"""
from __future__ import annotations

import collections
import urllib.parse as up

# Parameters that only say where a click came from. Everything else in a query
# string is presumed to identify the page (?slug=, ?v=, ?id=, ?story_fbid=):
# dropping an identity parameter merges different articles into one story,
# while keeping a tracking one only costs a missed merge.
_TRACKING = frozenset((
    "si", "ref", "ref_src", "ref_url", "referrer", "src", "source", "s", "cmpid",
    "smid", "smtyp", "fbclid", "gclid", "dclid", "msclkid", "yclid", "twclid",
    "igshid", "igsh", "mc_cid", "mc_eid", "mkt_tok", "_hsenc", "_hsmi", "ncid",
    "ocid", "cid", "sr_share", "share", "feature", "via", "trk", "taid",
))
_TRACKING_PREFIXES = ("utm_", "itm_", "pk_", "mtm_", "at_", "__")


def _is_tracking(key: str) -> bool:
    k = key.lower()
    return k in _TRACKING or k.startswith(_TRACKING_PREFIXES)


def canon_url(u: str) -> str | None:
    """Canonical article identity: host + path + identifying query parameters.

    Known tracking parameters are dropped; unknown ones are kept, sorted, so
    two URLs differing only in parameter order or click attribution match.
    """
    if not u:
        return None
    try:
        p = up.urlparse(u)
    except ValueError:
        return None
    host = p.netloc.lower().removeprefix("www.")
    if not host:
        return None
    path = p.path.rstrip("/")
    q = [(k, v) for k, v in up.parse_qsl(p.query, keep_blank_values=True) if not _is_tracking(k)]
    tail = ("?" + up.urlencode(sorted(q))) if q else ""
    return f"{host}{path}{tail}"


class DSU:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, x):
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def build(df, threads, *, max_url_group=25):
    """Return (unit_of_thread, units) where each unit is a list of thread ids."""
    n = len(threads)
    dsu = DSU(n)

    uri2t = {}
    for th in threads:
        for i in th.idxs:
            uri2t[df.iloc[i].uri] = th.tid

    for _, r in df.iterrows():
        if r.quote_uri:
            t = uri2t.get(r.quote_uri)
            if t is not None:
                dsu.union(uri2t[r.uri], t)

    by_url = collections.defaultdict(list)
    for th in threads:
        for u in th.urls:
            c = canon_url(u)
            if c:
                by_url[c].append(th.tid)
    for c, ts in by_url.items():
        # A URL shared by dozens of threads is a hub (a homepage, a shortener
        # landing page), not a story; merging on it fuses unrelated discussions.
        if 2 <= len(set(ts)) <= max_url_group:
            ts = sorted(set(ts))
            for t in ts[1:]:
                dsu.union(ts[0], t)

    groups = collections.defaultdict(list)
    for t in range(n):
        groups[dsu.find(t)].append(t)
    units = list(groups.values())
    unit_of = [0] * n
    for ui, ts in enumerate(units):
        for t in ts:
            unit_of[t] = ui
    return unit_of, units
