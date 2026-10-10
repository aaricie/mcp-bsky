"""Generate synthetic_timeline.jsonl (next to this script) — a deterministic,
fully synthetic 6-hour timeline for _test_index_frame.py.

No real accounts or posts: handles use the reserved .test TLD, links the
reserved .example TLD, and every post is shuffled topic words plus filler.
Run: python data/fixtures/make_synthetic_timeline.py
"""
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

rng = random.Random(20261010)
T0 = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)

TOPICS = [
    dict(words=["hormuz", "tanker", "strait", "navy", "seized"], domain="news-one.example",
         title="Tanker seized near the Strait of Hormuz", tag="hormuz"),
    dict(words=["kessler", "satellite", "debris", "orbit", "collision"], domain="space-daily.example",
         title="Debris cloud raises Kessler syndrome fears", tag="space"),
    dict(words=["odyssey", "nolan", "trailer", "imax", "homer"], domain="films.example",
         title="Nolan's Odyssey trailer lands", tag="odyssey"),
    dict(words=["bryansk", "drone", "refinery", "strike", "fire"], domain="wire.example",
         title="Drone strike hits Bryansk refinery", tag=None),
    dict(words=["hassett", "fed", "chair", "nomination", "senate"], domain="markets.example",
         title="Hassett nomination heads to the Senate", tag=None),
    dict(words=["sourdough", "starter", "hydration", "crumb", "bake"], domain=None, title=None, tag="bread"),
    dict(words=["wordle", "streak", "guess", "letters", "puzzle"], domain=None, title=None, tag="wordle"),
    dict(words=["heatwave", "toronto", "humidex", "advisory", "cooling"], domain="weather.example",
         title="Heat warning extended across Toronto", tag="heatwave"),
]
FILLER = ("honestly this is wild and I keep thinking about it today. not sure what happens next "
          "but people are talking about it everywhere. lol same. agreed with this take completely").split()
HANDLES = [f"user{i:02d}.test" for i in range(40)]

rows = []
n = 0


def make(handle, text, ts, reply_root=None, reply_parent=None, quote_uri=None, topic=None, likes=0):
    global n
    n += 1
    rkey = f"3syn{n:05d}"
    did = "did:plc:" + handle.split(".")[0].ljust(24, "x")
    uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    ext = topic and topic["domain"] and rng.random() < 0.35
    rows.append({
        "uri": uri, "cid": f"bafysyn{n}", "rkey": rkey, "did": did, "handle": handle,
        "created_at": ts.isoformat().replace("+00:00", "Z"), "text": text,
        "langs": "en", "reply_root": reply_root, "reply_parent": reply_parent, "quote_uri": quote_uri,
        "ext_uri": f"https://{topic['domain']}/story/{topic['words'][0]}?utm_source=bsky" if ext else None,
        "ext_title": topic["title"] if ext else None,
        "ext_desc": (" ".join(topic["words"][1:4]) + " report") if ext else None,
        "alt_text": f"photo of {topic['words'][1]}" if topic and rng.random() < 0.1 else None,
        "tags": topic["tag"] if topic and topic["tag"] and rng.random() < 0.3 else None,
        "n_likes": likes, "n_reposts": likes // 5, "n_replies": 0,
    })
    return uri


def sentence(topic):
    words = rng.sample(topic["words"], k=rng.randint(2, 4)) + rng.sample(FILLER, k=rng.randint(4, 9))
    rng.shuffle(words)
    return " ".join(words).capitalize() + "."


for topic in TOPICS:
    for _ in range(rng.randint(5, 9)):  # threads per topic
        ts = T0 + timedelta(minutes=rng.randint(0, 330))
        root = make(rng.choice(HANDLES), sentence(topic), ts, topic=topic, likes=rng.randint(0, 120))
        parent = root
        for _ in range(rng.randint(0, 5)):
            ts += timedelta(minutes=rng.randint(1, 20))
            parent = make(rng.choice(HANDLES), sentence(topic), ts, reply_root=root,
                          reply_parent=parent, topic=topic, likes=rng.randint(0, 10))
        if rng.random() < 0.4:
            make(rng.choice(HANDLES), sentence(topic), ts + timedelta(minutes=3), quote_uri=root,
                 topic=topic, likes=rng.randint(0, 30))
for _ in range(60):  # one-off chatter that should stay unindexed
    make(rng.choice(HANDLES), " ".join(rng.sample(FILLER, k=8)).capitalize() + ".",
         T0 + timedelta(minutes=rng.randint(0, 359)))

rows.sort(key=lambda r: r["created_at"], reverse=True)
out = Path(__file__).resolve().parent / "synthetic_timeline.jsonl"
out.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8", newline="\n")
print(len(rows), "posts ->", out)
