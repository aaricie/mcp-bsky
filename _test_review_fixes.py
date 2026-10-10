"""Regression checks for the October 2026 review findings — offline, real server.py.

Every network edge is faked (DNS, HTTP transports, SDK calls), so this runs
anywhere: `python _test_review_fixes.py`. Each check names the finding it
pins down.
"""
import asyncio
import socket
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpcore
import httpx
from atproto import models
from atproto_client import exceptions as atproto_exc
from atproto_client.client.session import Session
from atproto_client.models.common import XrpcError
from atproto_client.request import Response

from _test_support import server

PASS = 0
FAIL = 0


def check(desc, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f"FAIL: {desc}")


def raises(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type:
        return True
    except Exception as e:  # wrong type is a failure, but report it
        print(f"  (raised {type(e).__name__}: {e})")
        return False
    return False


class patch:
    """Minimal attribute patcher: with patch(obj, name, value): ..."""

    def __init__(self, obj, name, value):
        self.obj, self.name, self.value = obj, name, value

    def __enter__(self):
        self.old = getattr(self.obj, self.name)
        setattr(self.obj, self.name, self.value)
        return self.value

    def __exit__(self, *exc):
        setattr(self.obj, self.name, self.old)


def sdk_error(cls, status, error=None):
    content = XrpcError(error=error or "Err", message="m") if error else None
    return cls(Response(success=False, status_code=status, content=content, headers={}))


def Obj(**kw):
    return SimpleNamespace(**kw)


NOW = datetime.now(timezone.utc)


def post(uri, text="", ts=None, author="alice.test", reply_parent=None, likes=0, embed=None):
    ts = (ts or NOW).isoformat()
    reply = Obj(parent=Obj(uri=reply_parent), root=Obj(uri=reply_parent)) if reply_parent else None
    return Obj(
        uri=uri, cid="cid", indexed_at=ts, embed=embed,
        author=Obj(did=f"did:plc:{author.split('.')[0]}", handle=author, display_name=None),
        record=Obj(text=text, created_at=ts, reply=reply),
        like_count=likes, repost_count=0, reply_count=0,
    )


def item(p, reason=None):
    return Obj(post=p, reason=reason, reply=None)


def page(items, cursor):
    return Obj(feed=items, cursor=cursor)


server.client.me = Obj(did="did:plc:test", handle="test.bsky.social")
server.time.sleep = lambda s: None  # retries back off instantly in tests


# --- 1. SSRF: the connection itself is constrained to vetted addresses -------

def fake_getaddrinfo(answers):
    def gai(host, port, *a, **k):
        return [(socket.AF_INET6 if ":" in a_ else socket.AF_INET, socket.SOCK_STREAM, 6, "", (a_, port))
                for a_ in answers]
    return gai


connected = []
backend = server._PublicOnlyBackend()
with patch(httpcore.SyncBackend, "connect_tcp", lambda self, host, port, **k: connected.append(host) or "stream"):
    with patch(server.socket, "getaddrinfo", fake_getaddrinfo(["93.184.216.34", "10.0.0.5"])):
        check("1: mixed public+private DNS answers are refused",
              raises(httpcore.ConnectError, backend.connect_tcp, "rebind.test", 443))
    with patch(server.socket, "getaddrinfo", fake_getaddrinfo(["::1"])):
        check("1: IPv6 loopback answer is refused", raises(httpcore.ConnectError, backend.connect_tcp, "v6.test", 443))
    with patch(server.socket, "getaddrinfo", fake_getaddrinfo(["93.184.216.34"])):
        backend.connect_tcp("ok.test", 443)
        check("1: the vetted IP, not the hostname, is what gets connected", connected == ["93.184.216.34"])
check("1: IPv4-mapped loopback is not public", not server._is_public_ip("::ffff:127.0.0.1"))
check("1: literal private URL fails the pre-check", not server._is_safe_url("http://169.254.169.254/latest"))
with patch(server.socket, "getaddrinfo", fake_getaddrinfo(["127.0.0.1"])):
    check("1: end to end, a name resolving to loopback cannot be fetched",
          raises(httpx.ConnectError, server._public_http.get, "http://localtest.test/"))


# --- 2. Download limits are enforced while streaming --------------------------

def streaming_client(chunks, headers, consumed):
    def gen():
        for c in chunks:
            consumed.append(len(c))
            yield c

    def handler(request):
        return httpx.Response(200, headers=headers, content=gen())
    return httpx.Client(transport=httpx.MockTransport(handler))


consumed: list[int] = []
mb = b"x" * 1_000_000
with patch(server, "_public_http", streaming_client([mb] * 6, {"content-type": "image/png"}, consumed)):
    check("2: oversized image rejected", raises(ValueError, server._download_post_image, "https://cdn.test/a.png"))
check("2: image stream stopped at the limit, not after 6 MB", sum(consumed) <= server._POST_IMAGE_MAX_BYTES + 1_000_000)

consumed = []
with patch(server, "_public_http", streaming_client([mb] * 6, {"content-type": "image/png", "content-length": "6000000"}, consumed)):
    check("2: declared Content-Length over the cap rejected", raises(ValueError, server._download_post_image, "https://cdn.test/a.png"))
check("2: ... before reading any body", sum(consumed) == 0)

consumed = []
with patch(server, "_public_http", streaming_client([b"<html>"] , {"content-type": "text/html"}, consumed)):
    check("2: wrong content type rejected before the body",
          raises(ValueError, server._download_post_image, "https://cdn.test/a.png") and not consumed)

consumed = []
head = b'<html><head><meta property="og:title" content="Big Page"></head>'
with patch(server, "_public_http", streaming_client([head] + [mb] * 5, {"content-type": "text/html"}, consumed)):
    card = server._fetch_link_card("https://news.test/a")
check("2: link card HTML capped but still parsed", card["title"] == "Big Page" and sum(consumed) <= 2_100_000)


# --- 3/4. Retries: transient by type/status; exhausted retries are not "empty" --

calls = []


def failing(exc):
    def fetch(cursor=None, limit=100):
        calls.append(cursor)
        raise exc
    return fetch


check("4: InvokeTimeoutError (empty message) is transient", server._is_transient(atproto_exc.InvokeTimeoutError()))
check("4: 503 is transient", server._is_transient(sdk_error(atproto_exc.RequestException, 503)))
check("4: 400 is not transient", not server._is_transient(sdk_error(atproto_exc.BadRequestError, 400)))
check("4: 409 NetworkError is not transient", not server._is_transient(sdk_error(atproto_exc.NetworkError, 409)))

calls.clear()
check("3: three 500s on page one raise instead of returning []",
      raises(server.ToolError, server.fetch_by_count, failing(sdk_error(atproto_exc.RequestException, 500)), 10))
check("4: ... after exactly three attempts", len(calls) == 3)
calls.clear()
check("4: SDK timeouts are retried, then reported",
      raises(server.ToolError, server.fetch_by_count, failing(atproto_exc.InvokeTimeoutError()), 10) and len(calls) == 3)

state = {"n": 0}


def first_page_then_500(cursor=None, limit=100):
    state["n"] += 1
    if cursor is None:
        return page([item(post(f"at://a/p/{i}", "long enough text here")) for i in range(3)], "c1")
    raise sdk_error(atproto_exc.RequestException, 500)


got = server.fetch_by_count(first_page_then_500, 10)
check("3: a later-page failure keeps the posts and flags PARTIAL",
      len(got) == 3 and any("PARTIAL" in n for n in got.notes))
rendered = server._render_compact_feed(got, None, "none", 0)
check("3: the renderer shows the partial note", "PARTIAL" in rendered)


# --- 5/6. Session recovery and shared refresh lock ----------------------------

logins = []
server.client._session_dispatcher.set_session(Session(handle="test.bsky.social", did="did:plc:test",
                                                      access_jwt="a", refresh_jwt="r"))
with patch(server.client, "_should_refresh_session", lambda: True), \
        patch(server.client, "login", lambda *a, **k: logins.append(a)), \
        patch(server.client, "_refresh_and_set_session", failing(atproto_exc.InvokeTimeoutError())):
    check("5: a transient refresh failure propagates", raises(atproto_exc.InvokeTimeoutError, server._ensure_session))
    check("5: ... without a fresh login", not logins)
with patch(server.client, "_should_refresh_session", lambda: True), \
        patch(server.client, "login", lambda *a, **k: logins.append(a)), \
        patch(server.client, "_refresh_and_set_session",
              failing(sdk_error(atproto_exc.BadRequestError, 400, "ExpiredToken"))):
    server._ensure_session()
    check("5: an expired refresh token does trigger a login", len(logins) == 1)
server._login_state.update(retry_at=0.0, backoff=0.0, error="")

clones = []
orig_proxy = server.client.with_bsky_chat_proxy
with patch(server, "_ensure_session", lambda: None), \
        patch(server.client, "with_bsky_chat_proxy", lambda: clones.append(orig_proxy()) or clones[-1]):
    server._dm()
check("6: main client and chat clone share one refresh lock",
      server.client._refresh_lock is server._session_lock and clones[0]._refresh_lock is server._session_lock)
check("6: the shared lock is reentrant (login re-enters it)", isinstance(server._session_lock, type(threading.RLock())))


# --- 7. Cancelling a thread publish stops later writes; retries resume --------

sent = []
gate = threading.Event()
first_started = threading.Event()


def blocking_send(text, reply_to=None, embed=None, facets=None):
    sent.append(text)
    if len(sent) == 1:
        first_started.set()
        gate.wait(5)
    return Obj(uri=f"at://did:plc:test/app.bsky.feed.post/r{len(sent)}", cid=f"c{len(sent)}")


async def cancel_mid_thread():
    task = asyncio.create_task(server._run_tool(server._create_thread_impl, "one\n---\ntwo\n---\nthree"))
    await asyncio.to_thread(first_started.wait, 5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    gate.set()
    await asyncio.sleep(0.5)  # let the worker reach its next cancellation check


with patch(server.client, "send_post", blocking_send):
    asyncio.run(cancel_mid_thread())
check("7: cancellation stops the thread after the in-flight post", sent == ["one"])

sent.clear()
with patch(server.client, "send_post", blocking_send), patch(server, "_post_record_exists", lambda uri: True):
    out = server._create_thread_impl("one\n---\ntwo\n---\nthree")
check("7: retrying the same thread resumes after the published post", sent == ["two", "three"])
check("7: ... and says posts were reused", "reused" in out)

sent.clear()
with patch(server.client, "send_post", blocking_send), patch(server, "_post_record_exists", lambda uri: True):
    server._create_post_impl("hello world", None, None)
    again = server._create_post_impl("hello world", None, None)
check("7: an identical create_post retry does not post twice", sent == ["hello world"] and "already published" in again)


def failing_card(url):
    raise ValueError("injected preprocessing failure")


sent.clear()
with patch(server.client, "send_post", blocking_send), patch(server, "_build_external_embed", failing_card):
    try:
        server._create_thread_impl("First post\n---\nSecond post\n[embed: https://example.com/]")
        error = ""
    except ValueError as exc:
        error = str(exc)
check("7: a failure before post 2 (link card) still reports post 1 as live",
      sent == ["First post"] and "already published" in error and "/post/r1" in error)


# --- 8/9. Time windows: limit counts eligible posts; ranked feeds keep walking --

old = NOW - timedelta(hours=5)
short = post("at://a/p/short", "hi")
dup_page = [item(short), item(short, reason=Obj(py_type="app.bsky.feed.defs#reasonRepost", indexed_at=NOW.isoformat(), by=Obj(handle="b"))),
            item(post("at://a/p/long", "this one is long enough"))]
got = server.fetch_by_time(lambda cursor=None, limit=100: page(dup_page, None), 60, max_items=1, min_chars=10)
check("8: limit counts unique eligible posts, not raw items",
      any(i.post.uri == "at://a/p/long" for i in got))

pages = {None: page([item(post("at://a/p/old", "old post here", ts=old))], "p2"),
         "p2": page([item(post("at://a/p/new", "new post here"))], None)}
ranked = server.fetch_by_time(lambda cursor=None, limit=100: pages[cursor], 60, ordered=False)
check("9: a ranked feed keeps walking past an all-old page", [i.post.uri for i in ranked] == ["at://a/p/new"])
ordered = server.fetch_by_time(lambda cursor=None, limit=100: pages[cursor], 60, ordered=True)
check("9: a chronological source still stops at its first all-old page", list(ordered) == [])
endless = server.fetch_by_time(lambda cursor=None, limit=100: page([item(post("at://a/p/o", "old", ts=old))], f"{cursor}x"), 60)
check("9: an unordered walk ends after the old-page budget and says so",
      list(endless) == [] and any("not in time order" in n for n in endless.notes))
timeline = server._resolve_source("timeline")
check("9: the home timeline is marked chronological", getattr(timeline, "chronological", False))


# --- 10. Video views and 19. unavailable quotes -------------------------------

video = models.AppBskyEmbedVideo.View(cid="bafy", playlist="https://video.test/p.m3u8", alt="a cat")
vpost = Obj(embed=video)
check("10: a real Video.View is classified as video", server._get_embed_type(vpost) == ("video", 0))
check("10: ... with its alt text", server._get_embed_info(vpost) == "[Video] Alt: a cat")
qpost = Obj(embed=Obj(media=video, record=Obj(record=Obj(uri="at://q"))))
check("10: video inside record-with-media", server._get_embed_info(qpost) == "[Quote+video] Alt: a cat")

for model, word in ((models.AppBskyEmbedRecord.ViewNotFound(uri="at://x", not_found=True), "deleted"),
                    (models.AppBskyEmbedRecord.ViewBlocked(uri="at://x", blocked=True,
                                                           author=models.AppBskyFeedDefs.BlockedAuthor(did="did:plc:x")), "blocked"),
                    (models.AppBskyEmbedRecord.ViewDetached(uri="at://x", detached=True), "detached")):
    quoted = server._extract_quoted_post(Obj(embed=models.AppBskyEmbedRecord.View(record=model)))
    check(f"19: {model.py_type} gives an unavailable marker", quoted and word in quoted[3])


# --- 11/12. List maintenance --------------------------------------------------

def listrecords_pages(n):
    def get(endpoint, params=None):
        i = int(params.get("cursor") or 0)
        return {"records": [{"uri": f"at://did:plc:test/app.bsky.graph.listitem/{i}",
                             "value": {"list": "at://did:plc:test/app.bsky.graph.list/L", "subject": f"did:plc:m{i}"}}],
                "cursor": str(i + 1) if i + 1 < n else None}
    return get


writes = []
with patch(server, "_REPO_SCAN_HARD_CAP", 2), patch(server, "_xrpc_get", listrecords_pages(3)), \
        patch(server, "_xrpc_post", lambda *a, **k: writes.append(a)), \
        patch(server, "_owned_list_record", lambda ref: ("at://did:plc:test/app.bsky.graph.list/L", "L", {"cid": "c"})), \
        patch(server, "_get_raw_preferences", lambda: []):
    check("11: an incomplete membership scan aborts delete_list", raises(server.ToolError, server._delete_list_impl, "L"))
check("11: ... before any write", not writes)
with patch(server, "_xrpc_get", listrecords_pages(3)):
    records, complete = server._raw_repo_records("x")
check("11: a full scan reports complete", len(records) == 3 and complete)


def boom(*a, **k):
    raise AssertionError("profile lookup should not happen")


with patch(server, "_resolve_did", lambda h: h if h.startswith("did:") else boom()):
    resolved, failed, _ = server._resolve_removal_inputs(["did:plc:deactivated"])
check("12: removal takes a DID as given, no live profile needed",
      [r["did"] for r in resolved] == ["did:plc:deactivated"] and not failed)


# --- 13. Conversations paginate ------------------------------------------------

class FakeDM:
    def __init__(self):
        self.calls = 0

    def list_convos(self, params):
        self.calls += 1
        start = int(params.get("cursor") or 0)
        n = params["limit"]
        convos = [Obj(id=str(i)) for i in range(start, min(start + n, 250))]
        return Obj(convos=convos, cursor=str(start + n) if start + n < 250 else None)


dm = FakeDM()
convos, more = server._dm_list_convos(dm, {}, 150)
check("13: get_conversations follows cursors past the first 100", len(convos) == 150 and dm.calls == 2 and more)
convos, more = server._dm_list_convos(FakeDM(), {}, 1000)
check("13: ... and knows when it reached the end", len(convos) == 250 and not more)


# --- 14/15. Index header bounds; 16. canon_url; 17/24. selector ----------------

import pandas as pd  # noqa: E402
from comp import encode, select, units  # noqa: E402

t0 = pd.Timestamp("2026-10-10T14:03:27.900Z")
t1 = pd.Timestamp("2026-10-10T14:23:41.200Z")
head = encode.encode([], [], (t0, t1), n_posts=5, n_authors=3)
check("14: since is exact to the second", "since=2026-10-10T14:03:27Z" in head)
check("14: until is exclusive, one second past the newest post", "until=2026-10-10T14:23:42Z" in head)
check("14: a 20-minute window says 20m, not 0h", "(20m)" in head and "0h" not in head)
check("15: the following=1 hint has a broader fallback", "drop following" in head)

check("16: different ?slug= articles stay distinct",
      units.canon_url("https://news.test/article?slug=alpha") != units.canon_url("https://news.test/article?slug=beta"))
check("16: tracking parameters still collapse",
      units.canon_url("https://news.test/a?id=7&utm_source=x&fbclid=y") == units.canon_url("https://www.news.test/a/?id=7"))

import numpy as np  # noqa: E402

rng = np.random.default_rng(1)
masks = [rng.random(40) < 0.2 for _ in range(30)]
C = select.candidate_matrix([{"mask": m} for m in masks], 40)
check("17: candidate matrix is built sparse and matches the masks",
      np.array_equal(C.toarray().astype(bool), np.array(masks)))


def cand(q, mask, cost, quality):
    return {"q": q, "mask": np.array(mask), "cost": cost, "desc": quality, "coh": 0.5, "lift": 5.0,
            "bits": 12.0, "umass": np.ones(len(mask))}


cands = [cand(["broad"], [1, 1, 0, 0], 1, 0.4), cand(["specific", "topic"], [1, 1, 1, 0], 5, 1.0),
         cand(["other"], [0, 0, 0, 1], 1, 0.6)]
uu = [None] * 4
prev = None
monotone = True
for b in range(1, 15):
    got = [tuple(c["q"]) for c in select.choose(cands, uu, prefix_budget=b, min_new_mass=0.1)[0]]
    if prev is not None and got[:len(prev)] != prev:
        monotone = False
    prev = got
old_small = [tuple(c["q"]) for c in select.choose(cands, uu, budget=2, min_new_mass=0.1)[0]]
old_big = [tuple(c["q"]) for c in select.choose(cands, uu, budget=7, min_new_mass=0.1)[0]]
check("24: (the old budgeted selection really did reorder)", old_small[:1] != old_big[:1])
check("24: prefix_budget only ever appends entries as the budget grows", monotone)


# --- 18. Shared URLs with query strings / fragments ---------------------------

check("18: share-link query and fragment are not part of the rkey",
      server.resolve_bsky_url("https://bsky.app/profile/did:plc:abc/post/3kabc?ref_src=share#x")
      == "at://did:plc:abc/app.bsky.feed.post/3kabc")
check("18: a /liked-by style suffix still names the post",
      server.resolve_bsky_url("https://bsky.app/profile/did:plc:abc/post/3kabc/liked-by")
      == "at://did:plc:abc/app.bsky.feed.post/3kabc")
check("18: non-bsky hosts are rejected",
      raises(ValueError, server.resolve_bsky_url, "https://evil.test/profile/did:plc:abc/post/3k"))


# --- 20. Deep threads ----------------------------------------------------------

def thread_chain(depth):
    root = Obj(post=post("at://t/p/0", "root"), replies=[], parent=None)
    node = root
    for i in range(1, depth):
        child = Obj(post=post(f"at://t/p/{i}", f"reply {i}"), replies=[], parent=None)
        node.replies = [child]
        node = child
    return root


try:
    text = server._format_thread_node_text(thread_chain(1500))
    ok = "more repl" in text and "reply 1\n" in text
except RecursionError:
    ok = False
check("20: a 1,500-deep chain renders without RecursionError, capped with a count", ok)
small = server._format_thread_node_text(thread_chain(3))
check("20: small threads keep the old layout", "REPLIES (1)" in small and "reply 2" in small)


# --- 21. No unauthenticated public bind ---------------------------------------

with patch(server, "MCP_HOST", "0.0.0.0"):
    check("21: unauthenticated non-loopback startup is refused", raises(SystemExit, server._check_auth_exposure, []))
with patch(server, "MCP_HOST", "127.0.0.1"):
    server._check_auth_exposure([])
    check("21: loopback without auth is allowed", True)


# --- 22/23. Schema: CSV filters accepted, zero limits rejected ----------------

captured = {}


def fake_search(*args):
    captured["args"] = args
    return "ok"


async def call(name, args):
    try:
        res = await server.mcp.call_tool(name, args)
        return "ok", res
    except Exception as e:
        return type(e).__name__, str(e)


with patch(server, "_ensure_session", lambda: None):
    with patch(server, "_search_posts_impl", fake_search):
        status, _ = asyncio.run(call("search_posts", {"query": "x", "authors": "alice.test,bob.test"}))
        check("22: a comma-separated authors string passes the MCP schema",
              status == "ok" and captured["args"][4] == "alice.test,bob.test")
        check("22: ... and normalizes to a list", server._v2_list(captured["args"][4]) == ["alice.test", "bob.test"])
        status, _ = asyncio.run(call("search_posts", {"query": "x", "author": "alice.test", "since_minutes": 30,
                                                      "wait_for_previous": True}))
        check("shim: author/since_minutes aliases and wait_for_previous still work",
              status == "ok" and captured["args"][4] == ["alice.test"] and captured["args"][17] == 0.5)
        status, _ = asyncio.run(call("search_posts", {"query": "x", "authors": ["a.test"], "author": "b.test"}))
        check("shim: an alias plus its canonical name is an error", status != "ok")
    status, _ = asyncio.run(call("search_posts", {"query": "x", "limit": 0}))
    check("23: search_posts(limit=0) is rejected by the schema", status != "ok")
    status, _ = asyncio.run(call("get_bookmarks", {"page": 0}))
    check("23: get_bookmarks(page=0) is rejected by the schema", status != "ok")
    status, _ = asyncio.run(call("get_posts", {"mode": "index", "budget": 0}))
    check("23: get_posts(budget=0) is rejected by the schema", status != "ok")
    status, _ = asyncio.run(call("get_post_by_id", {"post_id": 987654}))
    check("errors: an unknown post ID is a tool error", status != "ok")


def guard_raises():
    raise atproto_exc.InvokeTimeoutError()


with patch(server, "_ensure_session", guard_raises), patch(server, "_search_posts_impl", fake_search):
    status, _ = asyncio.run(call("search_posts", {"query": "x"}))
    check("guard: a transient session-check failure with a session in hand lets the call run", status == "ok")
    with patch(server.client, "me", None):
        status, detail = asyncio.run(call("search_posts", {"query": "x"}))
    check("guard: with no session at all, the call fails with a sign-in error",
          status != "ok" and "Not signed in" in detail)


# --- 25/26. Cache keys and refresh atomicity ----------------------------------

cache: dict = {}
for did in ("did:plc:aaaaaaaa1", "did:plc:bbbbbbbb2", "did:plc:cccccccc3"):
    uri = f"at://{did}/app.bsky.feed.generator/news"
    cache[server._unique_cache_key(cache, "News", uri)] = uri
check("25: three 'News' feeds with the same rkey get three keys", len(cache) == 3)

server._publish_cache("feed", {"kept": "at://x"})


def prefs_down():
    raise httpx.ConnectError("down")


with patch(server, "_get_raw_preferences", prefs_down):
    raises(httpx.ConnectError, server.refresh_feed_cache)
check("26: a failed refresh keeps the last good snapshot", server.feed_cache == {"kept": "at://x"})


# --- 27. SQLite connections are closed; ID reuse cooldown ---------------------

with server._db(server._DB_PATH) as conn:
    pass
check("27: _db closes its connection", raises(sqlite3.ProgrammingError, conn.execute, "SELECT 1"))

[a_id] = server._db_store_posts(["at://a/p/A"])
with server._db(server._DB_PATH) as conn:
    conn.execute("UPDATE posts SET fetched_at = ?", (time.time() - server._POST_ID_TTL - 60,))
check("ID: an expired ID no longer resolves", server._db_lookup_post(a_id) is None)
[b_id] = server._db_store_posts(["at://a/p/B"])
check("ID: an expired ID is not handed to a different post before the cooldown", b_id != a_id)
[a_again] = server._db_store_posts(["at://a/p/A"])
check("ID: the same post seen again gets its old number back", a_again == a_id and server._db_lookup_post(a_id) == "at://a/p/A")


# --- 28. Compact rendering: only kept rows take IDs ---------------------------

with server._db(server._DB_PATH) as conn:
    conn.execute("DELETE FROM posts")
feed = [item(post(f"at://r/p/{i}", "x" * 200)) for i in range(50)]
with patch(server, "_RAW_OUTPUT_CHAR_BUDGET", 1500):
    out = server._render_compact_feed(feed, 60, "truncated_120", 0)
with server._db(server._DB_PATH) as conn:
    stored = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
shown = int(out.split(" posts", 1)[0])
check("28: rows past the cap get no ID", stored == shown and shown < 50 and "not shown" in out)
q = Obj(record=Obj(uri="at://q/p/1", author=Obj(handle="quoted.test", display_name=None),
                   value=Obj(text="quoted words " * 20, created_at=NOW.isoformat())))
qfeed = [item(post(f"at://r/q/{i}", "short" if i == 0 else "a much longer parent text " * 5, embed=q)) for i in range(3)]
out = server._render_compact_feed(qfeed, 60, "full_when_parent_text_short", 0)
check("28: quote table uses the first quoting post's text", ("quoted words " * 20).strip() in out)


# --- 29. Bookmarks: one enumeration serves several pages ----------------------

bm_calls = []


def bookmarks_api(endpoint, params=None):
    bm_calls.append(params.get("cursor"))
    start = int(params.get("cursor") or 0)
    return {"bookmarks": [{"subject": {"uri": f"at://b/p/{i}"}, "createdAt": NOW.isoformat()}
                          for i in range(start, min(start + 100, 250))],
            "cursor": str(start + 100) if start + 100 < 250 else None}


server._invalidate_bookmarks()
with patch(server, "_xrpc_get", bookmarks_api), \
        patch(server.client.app.bsky.feed, "get_posts", lambda params: Obj(posts=[])):
    p1 = server._get_bookmarks_impl(50, 1, "none")
    p3 = server._get_bookmarks_impl(50, 3, "none")
check("29: walking pages reuses the enumeration", len(bm_calls) == 3)
server._invalidate_bookmarks()
bm_calls.clear()
with patch(server.time, "monotonic", lambda: 50.0), patch(server, "_xrpc_get", bookmarks_api):
    try:
        records, complete = server._bookmark_records(10)
        ok = len(records) == 250 and complete
    except KeyError:
        ok = False
check("29: bookmarks work within two minutes of boot (monotonic clock near 0)", ok)
check("29: totals are exact", "of 250" in p1 and "page 3/5" in p3)
server._invalidate_bookmarks()


print(f"\n{PASS} passed, {FAIL} failed")
raise SystemExit(1 if FAIL else 0)
