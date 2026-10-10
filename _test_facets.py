"""Sanity-check rich-text facet detection (links / mentions / hashtags).

Self-contained: doesn't import server.py (avoids the Bluesky client login on
import). Mirrors _FACET_URL_RE / _FACET_MENTION_RE / _FACET_TAG_RE /
_trim_url_match and the byte-offset logic in server.py — if you change them
there, update this file too. Run: `python _test_facets.py`.

Facets are represented as plain dicts here; server.py builds the equivalent
models.AppBskyRichtextFacet objects.
"""
import re

_FACET_URL_RE = re.compile(
    r"https?://[^\s<>\"']+|www\.[^\s<>\"']+|\b[a-z0-9][-a-z0-9.]*\.[a-z]{2,}/[^\s<>\"']*",
    re.IGNORECASE,
)
_FACET_MENTION_RE = re.compile(r"(?<![\w@])@([a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,})")
_FACET_TAG_RE = re.compile(r"(?<![\w#])#([^\W\d_][\w]*)", re.UNICODE)
_URL_TRAILING_PUNCT = ".,;:!?'\")]}»…"

# Handles the fake resolver "knows"; anything else fails to resolve.
_KNOWN_HANDLES = {
    "alice.bsky.social": "did:plc:alice",
    "news.example.com": "did:plc:news",
}


def _trim_url_match(url: str) -> str:
    while url and url[-1] in _URL_TRAILING_PUNCT:
        if url[-1] == ")" and url.count("(") >= url.count(")"):
            break
        url = url[:-1]
    return url


def _detect_facets(text: str) -> list[dict]:
    facets: list[dict] = []
    claimed: list[tuple[int, int]] = []

    def overlaps(start, end):
        return any(s < end and start < e for s, e in claimed)

    def byte_slice(start, end):
        return (len(text[:start].encode("utf-8")), len(text[:end].encode("utf-8")))

    for m in _FACET_URL_RE.finditer(text):
        url = _trim_url_match(m.group(0))
        if not url:
            continue
        start, end = m.start(), m.start() + len(url)
        uri = url if url.lower().startswith(("http://", "https://")) else f"https://{url}"
        facets.append({"type": "link", "index": byte_slice(start, end), "uri": uri})
        claimed.append((start, end))

    for m in _FACET_MENTION_RE.finditer(text):
        start, end = m.start(), m.end()
        if overlaps(start, end):
            continue
        did = _KNOWN_HANDLES.get(m.group(1))
        if did is None:
            continue
        facets.append({"type": "mention", "index": byte_slice(start, end), "did": did})
        claimed.append((start, end))

    for m in _FACET_TAG_RE.finditer(text):
        start, end = m.start(), m.end()
        if overlaps(start, end):
            continue
        tag = m.group(1)
        if len(tag.encode("utf-8")) > 640:
            continue
        facets.append({"type": "tag", "index": byte_slice(start, end), "tag": tag})
        claimed.append((start, end))

    return facets


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
