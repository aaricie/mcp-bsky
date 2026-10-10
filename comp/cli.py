"""compress a Bluesky timeline capture into a research index.

    python -m comp data/raw/posts/timeline-00000.parquet --budget 600
"""
from __future__ import annotations

import argparse
import collections
import sys

import numpy as np

from . import data, encode, evaluate, pipeline


def top_domains(df, k=8):
    from .units import canon_url
    c = collections.Counter()
    for u in df.ext_uri:
        if u:
            cu = canon_url(u)
            if cu:
                c[cu.split("/")[0]] += 1
    return [d for d, _ in c.most_common(k)]


def top_authors(df, units, chosen, k=6):
    c = collections.Counter(df.handle)
    return [h.removesuffix(".bsky.social") for h, _ in c.most_common(k)]


def main(argv=None):
    ap = argparse.ArgumentParser(prog="comp", description=__doc__)
    ap.add_argument("path")
    ap.add_argument("--budget", type=int, default=400,
                    help="token budget for the rendered artifact (default 400)")
    ap.add_argument("--max-entries", type=int, default=None)
    ap.add_argument("--sep", default="; ",
                    help=r"entry separator; '; ' costs ~0.5 tok/entry less than '\n'")
    ap.add_argument("--counts", action="store_true", help="append post counts")
    ap.add_argument("--no-header", action="store_true")
    ap.add_argument("--no-extras", action="store_true",
                    help="omit source-domain, loudest-account and residual lines")
    ap.add_argument("--stats", action="store_true", help="print metrics to stderr")
    a = ap.parse_args(argv)

    ctx = pipeline.prepare(a.path)
    R = pipeline.run(None, ctx=ctx, budget=a.budget, max_entries=a.max_entries)
    df, uu = ctx["df"], ctx["units"]
    w = data.window(df)
    sep = a.sep.encode().decode("unicode_escape")

    txt = encode.encode(
        R["chosen"], uu, w,
        n_posts=len(df), n_authors=df.handle.nunique(),
        header=not a.no_header, sep=sep, counts=a.counts,
        residual=None if a.no_extras else encode.residual_line(R["chosen"], uu),
        domains=None if a.no_extras else top_domains(df),
        top_authors=None if a.no_extras else top_authors(df, uu, R["chosen"]),
    )
    print(txt)

    if a.stats:
        rep = encode.report(txt)
        cv = evaluate.coverage(R["chosen"], uu)
        base = baselines(df)
        print(f"\n[entries {len(R['chosen'])}  tokens {rep['tokens']}  "
              f"chars {rep['chars']}]", file=sys.stderr)
        print(f"[coverage units {cv['units']:.1%}  posts {cv['posts']:.1%}  "
              f"engagement {cv['engagement']:.1%}]", file=sys.stderr)
        for k, v in base.items():
            print(f"[vs {k}: {v:,} tok -> {v/rep['tokens']:.0f}x]", file=sys.stderr)
    return txt


def baselines(df):
    """Token cost of the alternatives an LLM would otherwise be handed."""
    import json
    from .tok import ntok
    recs = df.to_dict("records")
    rows = []
    for i, r in enumerate(recs):
        t = (r["text"] or "").replace("|", "¦").replace("\n", " ")
        eng = (f'{r["n_likes"]},{r["n_reposts"]},{r["n_replies"]}'
               if (r["n_likes"] or 0) + (r["n_reposts"] or 0) + (r["n_replies"] or 0) >= 10
               else "")
        ref = "Re" if r["reply_root"] else ("Q" if r["quote_uri"] else "")
        rows.append(f'{i}|{r["handle"]}|{str(r["created_at"])[11:16]}|{eng}|{ref}|{t}')
    keep = ["handle", "created_at", "text", "reply_root", "quote_uri", "ext_uri",
            "ext_title", "n_likes", "n_reposts", "n_replies"]
    return {
        "raw JSON export": ntok(json.dumps(recs, ensure_ascii=False, default=str)),
        "trimmed JSON": ntok(json.dumps(
            [{k: r[k] for k in keep if r[k] not in (None, "", 0)} for r in recs],
            ensure_ascii=False, default=str)),
        "MCP compact rows": ntok("\n".join(rows)),
        "post text only": ntok("\n".join((r["text"] or "").replace("\n", " ")
                                         for r in recs)),
    }


if __name__ == "__main__":
    main()
