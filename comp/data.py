"""Load a timeline capture and turn it into the units the compressor works on.

The atomic unit is a *thread*, not a post: a discussion is a thread, and a
single pointer to a thread root dereferences (via get_thread) to every reply in
it. Posts whose root fell outside the capture window still group correctly —
matching any reply is enough to find the thread.
"""
from __future__ import annotations

import re
import urllib.parse as up
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# Fields Bluesky's search backend (Palomar/OpenSearch) actually indexes.
# Verified empirically: link-card titles and image alt text ARE searchable —
# a post reading only "CentCom's (and NATO's) nightmare." matches the query
# "Pakistan", which appears solely in its link card.
SEARCHABLE = ("text", "ext_title", "ext_desc", "alt_text", "tags")

_WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)

# Bluesky renders links as truncated display text ("www.reuters.com/world/asia-p…",
# "ft.trib.al/YYVBRco"), which otherwise floods the vocabulary with host
# fragments — the first run picked "www", "reut", "rs", "trib" and "ly" as topics.
# Any dotted host-like run, with or without a scheme or path, is removed.
_URLISH = re.compile(
    r"""(?:https?://\S+)                       # explicit scheme
      | (?:\b[\w-]+(?:\.[\w-]+)+ (?:/\S*|\.{2,3}|\b))   # host[.host]+ [path|ellipsis]
    """,
    re.VERBOSE | re.UNICODE,
)
_HANDLEISH = re.compile(r"@[\w.-]+")


def analyze(s) -> list[str]:
    """Approximate OpenSearch's standard analyzer: lowercase, unicode word split."""
    if not s or not isinstance(s, str):
        return []
    s = _URLISH.sub(" ", s)
    s = _HANDLEISH.sub(" ", s)
    return [m.group(0).lower() for m in _WORD.finditer(s)]


_TOKPOS = re.compile(r"([.!?:;\n]\s*)?([^\W_]+(?:['’][^\W_]+)*)", re.UNICODE)


def cap_scan(s, cap, low):
    """Count mid-sentence capitalised vs lowercase occurrences of each word.

    Sentence-initial words are skipped (their case says nothing) and ALLCAPS is
    skipped (emphasis, not a name). Shared by the background builder and the
    per-timeline fallback so both measure the same thing.
    """
    if not s or not isinstance(s, str):
        return
    s = _HANDLEISH.sub(" ", _URLISH.sub(" ", s))
    first = True
    for m in _TOKPOS.finditer(s):
        boundary, w = m.group(1) is not None, m.group(2)
        if first or boundary:
            first = False
            continue
        if len(w) < 2 or w.isupper():
            continue
        (cap if w[0].isupper() else low)[w.lower()] += 1


@dataclass
class Thread:
    tid: int
    root_uri: str
    root_rkey: str | None          # None when the root fell outside the window
    root_handle: str | None
    idxs: list[int]                # row indices in the post frame
    n_posts: int
    handles: list[str]
    t0: float                      # hours from window start
    t1: float
    likes: int
    reposts: int
    replies: int
    domains: list[str] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    langs: list[str] = field(default_factory=list)
    terms: set[str] = field(default_factory=set)          # analyzed, searchable
    root_terms: set[str] = field(default_factory=set)     # from the root post only
    tf: dict = field(default_factory=dict)   # term -> how many posts in the thread use it
    text: str = ""                                        # for eval/reference only


def _split(v) -> list[str]:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return []
    return [x for x in re.split(r"[\t,;| ]+", str(v)) if x]


def post_termsets(df) -> list[set]:
    """Exactly what Bluesky's index holds for each post.

    Search matches a conjunction against a *single* post, never against a
    thread's pooled vocabulary. Building postlists from pooled thread terms
    produced entries no search could ever return — 19 of 193 in one measured
    run, including "democratic obama", whose two words appear in the same
    thread but never in the same post.
    """
    out = []
    for _, r in df.iterrows():
        tk = set()
        for f in SEARCHABLE:
            tk.update(analyze(r[f]))
        out.append(tk)
    return out


def load(path: str) -> tuple[pd.DataFrame, list[Thread]]:
    return from_frame(pd.read_parquet(path))


def from_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, list[Thread]]:
    """Thread-building half of `load`, over an already-materialised frame.

    Split out so a live capture (the MCP server hands over PostViews, never a
    parquet file) enters the pipeline through exactly the same code path the
    offline captures do.
    """
    df = df.copy()
    for c in ("text", "langs", "reply_root", "reply_parent", "quote_uri", "embed_type",
              "ext_uri", "ext_title", "ext_desc", "links", "tags", "mentions",
              "alt_text", "handle", "rkey", "uri", "cid", "did", "created_at"):
        if c in df.columns:
            df[c] = df[c].astype(object).where(pd.notna(df[c]), None)
    df = df.reset_index(drop=True)

    ts = pd.to_datetime(df.created_at, format="mixed", utc=True)
    t0 = ts.min()
    df["hrs"] = (ts - t0).dt.total_seconds() / 3600.0
    df["thread_uri"] = df.reply_root.where(df.reply_root.notna(), df.uri)

    uri2row = {u: i for i, u in enumerate(df.uri)}
    threads: list[Thread] = []
    for tid, (turi, g) in enumerate(df.groupby("thread_uri", sort=False)):
        idxs = list(g.index)
        rrow = uri2row.get(turi)
        terms, root_terms, doms, urls, tags, langs = set(), set(), [], [], [], []
        tf: dict[str, int] = {}
        for i in idxs:
            r = df.iloc[i]
            toks = set()
            for f in SEARCHABLE:
                toks.update(analyze(r[f]))
            terms |= toks
            for tk in toks:
                tf[tk] = tf.get(tk, 0) + 1
            if i == rrow:
                root_terms = toks
            if r.ext_uri:
                try:
                    d = up.urlparse(r.ext_uri).netloc.lower().removeprefix("www.")
                    if d:
                        doms.append(d)
                        urls.append(r.ext_uri)
                except ValueError:
                    pass
            tags += [t.lower() for t in _split(r.tags)]
            langs += _split(r.langs)
        if rrow is None:                      # root outside window: use earliest reply
            rrow = idxs[0]
            root_terms = terms
            root_known = False
        else:
            root_known = True
        threads.append(Thread(
            tid=tid, root_uri=turi,
            root_rkey=df.iloc[rrow].rkey if root_known else None,
            root_handle=df.iloc[rrow].handle if root_known else None,
            idxs=idxs, n_posts=len(idxs),
            handles=list(g.handle),
            t0=float(g.hrs.min()), t1=float(g.hrs.max()),
            likes=int(g.n_likes.sum()), reposts=int(g.n_reposts.sum()),
            replies=int(g.n_replies.max()),
            domains=doms, urls=urls, tags=tags, langs=langs,
            terms=terms, root_terms=root_terms, tf=tf,
            text=" ⏎ ".join(str(x) for x in g.text if x),
        ))
    return df, threads


def window(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    ts = pd.to_datetime(df.created_at, format="mixed", utc=True)
    return ts.min(), ts.max()
