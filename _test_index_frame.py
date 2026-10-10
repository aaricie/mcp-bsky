"""Round-trip check for the atproto -> comp/ adapter (`_postviews_to_frame`).

The index is only as good as the columns it is handed: comp/data.py's
SEARCHABLE fields are what Bluesky's own search engine indexes, so a column
silently dropped or misnamed here produces entries no search can resolve — and
nothing about the output would look wrong.

So this test takes a capture as ground truth, reshapes each row into an
object with the same attribute layout `atproto` gives us
(PostView.record.text, .embed.external.title, ...), pushes that through the
real `server._postviews_to_frame`, and asserts the resulting index is
IDENTICAL to the one built from the capture directly.

The default capture is the committed, fully synthetic
data/fixtures/synthetic_timeline.jsonl (302 posts, regenerate with
data/fixtures/make_synthetic_timeline.py). A real capture from the compressor
lab (parquet, e.g. the 3,047-post timeline) can be passed as argv[1] or via
COMP_TIMELINE_CAPTURE for a stronger check.

Run:  python _test_index_frame.py [path/to/timeline.parquet|.jsonl]
Needs pandas/numpy/scipy/tiktoken and data/bg/*.pkl.
"""
import os
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
_ARG = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else None  # before the chdir below
DEFAULT_CAPTURE = Path(os.environ.get(
    "COMP_TIMELINE_CAPTURE", HERE / "data" / "fixtures" / "synthetic_timeline.jsonl")).resolve()

from _test_support import server  # noqa: E402  (no login, no MCP, no network)


def load_capture(path: Path) -> pd.DataFrame:
    if path.suffix == ".jsonl":
        return pd.read_json(path, lines=True, dtype=False)
    return pd.read_parquet(path)


def load_server_functions() -> dict:
    return vars(server)


class Obj:
    """Attribute bag standing in for an atproto model."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def row_to_feed_item(row) -> Obj:
    """Reshape a capture row into the PostView layout the adapter reads."""
    def val(name):
        v = getattr(row, name, None)
        return None if v is None or (isinstance(v, float) and v != v) else v

    reply = None
    if val("reply_root"):
        reply = Obj(root=Obj(uri=val("reply_root")),
                    parent=Obj(uri=val("reply_parent") or val("reply_root")))

    tags = val("tags")
    record = Obj(
        text=val("text") or "",
        created_at=str(val("created_at")),
        langs=(str(val("langs")).split() if val("langs") else None),
        reply=reply,
        # Half through the plain `tags` field, half through richtext facets, so
        # both paths in _record_tags are exercised on real data.
        tags=None,
        facets=None,
    )
    if tags:
        parts = str(tags).split()
        record.tags = parts[::2] or None
        facet_tags = parts[1::2]
        if facet_tags:
            record.facets = [Obj(features=[Obj(tag=t)]) for t in facet_tags]

    embed = None
    if val("ext_uri"):
        embed = Obj(external=Obj(uri=val("ext_uri"),
                                 title=val("ext_title") or "",
                                 description=val("ext_desc") or ""))
    if val("quote_uri"):
        quoted = Obj(record=Obj(uri=val("quote_uri")))
        embed = Obj(record=quoted, media=embed) if embed else quoted and Obj(record=quoted)

    if val("alt_text"):
        alts = [Obj(alt=str(val("alt_text")))]
        if embed is None:
            embed = Obj(images=alts)
        elif getattr(embed, "media", None) is None and not hasattr(embed, "external"):
            embed.media = Obj(images=alts)

    post = Obj(
        uri=val("uri"), cid=val("cid") or "",
        author=Obj(did=val("did") or "", handle=val("handle") or "?"),
        record=record, embed=embed,
        like_count=int(val("n_likes") or 0),
        repost_count=int(val("n_reposts") or 0),
        reply_count=int(val("n_replies") or 0),
        indexed_at=str(val("created_at")),
    )
    return Obj(post=post)


def index_entries(frame, bg, priors, budget=400):
    from comp import pipeline
    ctx = pipeline.prepare(None, frame=frame, bg=bg, priors=priors)
    return [c["q"] for c in pipeline.run(None, ctx=ctx, budget=budget)["chosen"]]


def main():
    capture = _ARG or DEFAULT_CAPTURE
    if not capture.exists():
        raise SystemExit(f"capture not found: {capture}")

    from comp.bg import Background
    from comp.vocab import Priors
    bg, priors = Background(), Priors()

    truth = load_capture(capture)
    ns = load_server_functions()
    rebuilt = ns["_postviews_to_frame"](
        [(row_to_feed_item(r), []) for r in truth.itertuples(index=False)]
    )

    failures = []

    # 1. Every column comp/data.py reads must survive the round trip.
    from comp.data import SEARCHABLE
    for col in (*SEARCHABLE, "uri", "rkey", "handle", "created_at",
                "reply_root", "quote_uri", "ext_uri",
                "n_likes", "n_reposts", "n_replies"):
        if col not in rebuilt.columns:
            failures.append(f"column {col!r} missing from _postviews_to_frame output")

    # 2. The searchable text must be preserved, not just present. Compare the
    #    analyzed token sets — that is exactly what the index is built from.
    if not failures:
        from comp.data import analyze
        for col in SEARCHABLE:
            a = set().union(*[set(analyze(v)) for v in truth[col]]) if col in truth else set()
            b = set().union(*[set(analyze(v)) for v in rebuilt[col]])
            lost = a - b
            if lost:
                failures.append(f"{col}: {len(lost)} terms lost, e.g. {sorted(lost)[:8]}")

    # 3. Same posts, same threading.
    if list(truth.uri) != list(rebuilt.uri):
        failures.append("uri column diverged")
    truth_roots = truth.reply_root.where(pd.notna(truth.reply_root), None).tolist()
    if truth_roots != rebuilt.reply_root.tolist():
        failures.append("reply_root diverged — threads would group differently")

    # 4. The reported window must come from the SERVER's clock, not the
    #    author's. record.created_at is client-set: one live fetch carried a
    #    post backdated to 2023, which stretched a 6 h window to "+1045.1d" and
    #    would have pointed every expansion search at the wrong bounds.
    liar = truth.iloc[0].copy()
    liar.text = "a backdated post that lies about when it was written"
    items = [(row_to_feed_item(r), []) for r in truth.itertuples(index=False)]
    lying_item = row_to_feed_item(liar)
    lying_item.post.record.created_at = "2023-09-28T01:17:00Z"   # the lie
    lying_item.post.indexed_at = str(liar.created_at)            # the truth
    items.append((lying_item, []))

    from comp.data import window as comp_window
    with_liar = ns["_postviews_to_frame"](items)
    span_hours = (lambda w: (w[1] - w[0]).total_seconds() / 3600)(comp_window(with_liar))
    honest_span = (lambda w: (w[1] - w[0]).total_seconds() / 3600)(comp_window(rebuilt))
    if span_hours > honest_span + 1:
        failures.append(
            f"backdated created_at leaked into the window: {span_hours:.0f}h "
            f"vs the true {honest_span:.0f}h — use _get_feed_item_timestamp"
        )
    else:
        print(f"window resists a backdated post: {span_hours:.1f}h (true {honest_span:.1f}h)")

    # 5. The payoff: identical index out of both frames.
    if not failures:
        expected = index_entries(truth, bg, priors)
        actual = index_entries(rebuilt, bg, priors)
        if expected != actual:
            failures.append(
                f"index diverged: {len(expected)} vs {len(actual)} entries; "
                f"only in parquet={[e for e in expected if e not in actual][:5]}, "
                f"only in adapter={[a for a in actual if a not in expected][:5]}"
            )
        else:
            print(f"index identical: {len(expected)} entries from both frames")

    for f in failures:
        print(f"FAIL: {f}")
    print(f"\n{len(truth)} posts, {'0 failures — adapter is faithful' if not failures else str(len(failures)) + ' failures'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
