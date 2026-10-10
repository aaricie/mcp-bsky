"""End-to-end: a timeline capture in, a ranked list of named discussions out."""
from __future__ import annotations

import collections
from dataclasses import dataclass, field

import numpy as np

from . import data, mine, select, units, vocab
from .bg import Background
from .coherence import Coherence
from .postidx import PostIndex


@dataclass
class Unit:
    tids: list
    terms: set
    n_posts: int
    likes: int
    reposts: int
    replies: int
    handles: list
    t0: float
    t1: float
    domains: list = field(default_factory=list)
    langs: list = field(default_factory=list)
    root_rkey: str | None = None
    root_handle: str | None = None
    txt: str = ""
    tf: dict = field(default_factory=dict)
    root_terms: set = field(default_factory=set)
    post_rows: list = field(default_factory=list)


def build_units(df, threads, use_article_edges=True):
    _, U = units.build(df, threads, max_url_group=25 if use_article_edges else 0)
    uu = []
    for ts in U:
        th = [threads[t] for t in ts]
        main = max(th, key=lambda x: x.likes + x.n_posts)
        tf: dict[str, int] = {}
        for x in th:
            for k, v in x.tf.items():
                tf[k] = tf.get(k, 0) + v
        uu.append(Unit(
            tids=ts,
            terms=set().union(*[x.terms for x in th]),
            n_posts=sum(x.n_posts for x in th),
            likes=sum(x.likes for x in th),
            reposts=sum(x.reposts for x in th),
            replies=max(x.replies for x in th),
            handles=sorted({h for x in th for h in x.handles}),
            t0=min(x.t0 for x in th), t1=max(x.t1 for x in th),
            domains=[d for x in th for d in x.domains],
            langs=[l for x in th for l in x.langs],
            root_rkey=main.root_rkey, root_handle=main.root_handle,
            root_terms=set().union(*[x.root_terms for x in th]),
            txt=" ⏎ ".join(x.text[:160] for x in th)[:400],
            tf=tf,
            post_rows=[i for x in th for i in x.idxs],
        ))
    return uu


def headline_terms(df, uu, threads):
    """Fraction of units whose link-card headline contains each term."""
    cnt = collections.Counter()
    for u in uu:
        ht = set()
        for t in u.tids:
            for i in threads[t].idxs:
                ht.update(data.analyze(df.iloc[i].ext_title))
        cnt.update(ht)
    n = max(1, len(uu))
    return {t: c / n * 40 for t, c in cnt.items()}      # scaled to ~[0,1] for common headline terms


def prepare(path=None, *, frame=None, bg=None, priors=None, per_post=14,
            max_len=3, max_cost=7, min_sup=2, use_article_edges=True):
    """Everything up to (but not including) selection — the expensive part.

    Takes either a parquet `path` or an in-memory `frame` with the same columns.

    `use_article_edges=False` withholds the shared-article must-links so they can
    serve as held-out supervision: the selector must then rejoin a story from
    vocabulary alone, with no link telling it the posts belong together.
    """
    if frame is not None:
        df, threads = data.from_frame(frame)
    else:
        df, threads = data.load(path)
    priors = priors or vocab.Priors()
    bg = bg or Background()
    lc = vocab.local_caprates(df, threads)
    uu = build_units(df, threads, use_article_edges)
    n = len(uu)

    terms, M, tidx = vocab.build(uu, priors, lc, min_df=1, max_df_frac=0.15,
                                 max_cost=5, bgidx=bg)
    dfu = M.sum(1)
    rare = (dfu >= 1) & (dfu <= max(4, int(0.05 * n)))
    coh = Coherence(M & rare[:, None])

    post_terms = data.post_termsets(df)
    post2unit = np.zeros(len(df), dtype=np.int32)
    for ui, u in enumerate(uu):
        for r in u.post_rows:
            post2unit[r] = ui
    pidx = PostIndex(post_terms, post2unit, len(uu), tidx)

    cands = mine.mine(uu, terms, M, tidx, priors, lc, pidx, post_terms,
                      per_post=per_post, max_len=max_len, min_sup=min_sup,
                      max_cost=max_cost)
    cands += mine.solo(uu, terms, M, tidx, priors, lc, pidx, post_terms,
                       max_len=max_len, max_cost=max_cost)
    ht = headline_terms(df, uu, threads)
    cands = select.annotate(cands, uu, priors, bg, coh, lc, terms, head_terms=ht)
    return dict(df=df, threads=threads, units=uu, terms=terms, M=M, tidx=tidx,
                coh=coh, cands=cands, priors=priors, bg=bg, lc=lc, head_terms=ht)


def run(path, *, budget=None, max_entries=None, ctx=None, gate_kw=None,
        choose_kw=None, **kw):
    ctx = ctx or prepare(path, **kw)
    gkw = {"n_units": len(ctx["units"]), **(gate_kw or {})}
    kept = select.gate(ctx["cands"], **gkw)
    chosen, spent = select.choose(kept, ctx["units"], budget=budget,
                                  max_entries=max_entries, **(choose_kw or {}))
    return {**ctx, "kept": kept, "chosen": chosen, "spent": spent}
