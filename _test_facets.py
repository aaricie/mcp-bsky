"""Sanity-check rich-text facet detection (links / mentions / hashtags).

Runs the real server._detect_facets with a fake handle resolver, so there is
no copy of the regexes to keep in sync. Run: `python _test_facets.py`.

Facets are flattened to plain dicts for the assertions below.
"""
from _test_support import server

# Handles the fake resolver "knows"; anything else fails to resolve.
_KNOWN_HANDLES = {
    "alice.bsky.social": "did:plc:alice",
    "news.example.com": "did:plc:news",
}


def _fake_resolve(handle: str) -> str:
    return _KNOWN_HANDLES[handle]  # KeyError = unresolvable, like the API failing


def _detect_facets(text: str) -> list[dict]:
    out = []
    for facet in server._detect_facets(text, resolve_handle=_fake_resolve) or []:
        feature = facet.features[0]
        index = (facet.index.byte_start, facet.index.byte_end)
        if hasattr(feature, "uri"):
            out.append({"type": "link", "index": index, "uri": feature.uri})
        elif hasattr(feature, "did"):
            out.append({"type": "mention", "index": index, "did": feature.did})
        else:
            out.append({"type": "tag", "index": index, "tag": feature.tag})
    return out


def _slice_by_bytes(text: str, index: tuple[int, int]) -> str:
    raw = text.encode("utf-8")
    return raw[index[0]:index[1]].decode("utf-8")


PASS = 0
FAIL = 0


def check(desc, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f"FAIL: {desc}")


def facet_of(facets, ftype, n=0):
    matches = [f for f in facets if f["type"] == ftype]
    return matches[n] if len(matches) > n else None


# --- URLs ---

f = _detect_facets("read this: https://example.com/a?b=c#frag now")
link = facet_of(f, "link")
check("scheme URL detected", link and link["uri"] == "https://example.com/a?b=c#frag")
check("scheme URL byte range matches text",
      link and _slice_by_bytes("read this: https://example.com/a?b=c#frag now", link["index"])
      == "https://example.com/a?b=c#frag")
check("no tag facet for #frag inside URL", facet_of(f, "tag") is None)

f = _detect_facets("see www.example.com, ok")
link = facet_of(f, "link")
check("www URL detected, trailing comma trimmed", link and link["uri"] == "https://www.example.com")

f = _detect_facets("docs at en.wikipedia.org/wiki/Foo_(bar) here")
link = facet_of(f, "link")
check("balanced paren kept in URL", link and link["uri"].endswith("Foo_(bar)"))

f = _detect_facets("(see example.com/path.)")
link = facet_of(f, "link")
check("unbalanced trailing paren+dot trimmed", link and link["uri"] == "https://example.com/path")

f = _detect_facets("Sentence ends here i.e. nothing to link")
check("abbreviation not linkified", facet_of(f, "link") is None)

f = _detect_facets("Emoji 🚀 then https://x.com/y end")
link = facet_of(f, "link")
check("byte offsets correct after multi-byte emoji",
      link and _slice_by_bytes("Emoji 🚀 then https://x.com/y end", link["index"]) == "https://x.com/y")

# --- Mentions ---

f = _detect_facets("hi @alice.bsky.social!")
m = facet_of(f, "mention")
check("mention resolved", m and m["did"] == "did:plc:alice")
check("mention range excludes trailing !",
      m and _slice_by_bytes("hi @alice.bsky.social!", m["index"]) == "@alice.bsky.social")

f = _detect_facets("hi @unknown.example.zz")
check("unresolvable mention left as plain text", facet_of(f, "mention") is None)

f = _detect_facets("mail me at someone@gmail.com please")
check("email not treated as mention", facet_of(f, "mention") is None)

f = _detect_facets("just @wordwithoutdot here")
check("dotless mention skipped", facet_of(f, "mention") is None)

# --- Hashtags ---

f = _detect_facets("loving #Bluesky and #ai2026 today")
t0, t1 = facet_of(f, "tag", 0), facet_of(f, "tag", 1)
check("first hashtag detected", t0 and t0["tag"] == "Bluesky")
check("hashtag with digits detected", t1 and t1["tag"] == "ai2026")

f = _detect_facets("issue #123 is numeric")
check("pure-numeric tag skipped", facet_of(f, "tag") is None)

f = _detect_facets("multilingual #日本語 tag")
t = facet_of(f, "tag")
check("CJK hashtag detected with correct bytes",
      t and _slice_by_bytes("multilingual #日本語 tag", t["index"]) == "#日本語")

# --- Mixed / overlap ---

text = "by @alice.bsky.social: https://news.example.com/story #breaking"
f = _detect_facets(text)
check("mixed text has all three facets",
      facet_of(f, "link") and facet_of(f, "mention") and facet_of(f, "tag"))
for fc in f:
    label = {"link": "uri", "mention": "did", "tag": "tag"}[fc["type"]]
    check(f"byte slice for {fc['type']} decodes cleanly",
          isinstance(_slice_by_bytes(text, fc["index"]), str))

f = _detect_facets("plain text with nothing special")
check("plain text yields no facets", not f)

print(f"\n{PASS} passed, {FAIL} failed")
raise SystemExit(1 if FAIL else 0)
