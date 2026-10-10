"""Bluesky MCP server — exposes a Bluesky account to LLM clients over MCP.

Read tools (posts, threads, profiles, social graph, notifications, search,
bookmarks, likes, drafts), list/draft management, and post/thread creation.
Single file by design; see CLAUDE.md for architecture and deployment notes.
"""
import asyncio
import base64
import contextvars
import copy
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import socket
import sqlite3
import threading
from collections import Counter
from contextlib import asynccontextmanager, closing, contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Annotated, Literal
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import time

import grapheme
import httpcore
import httpx
from atproto import Client, Request as AtprotoRequest, models
from atproto_client import exceptions as atproto_exc
from atproto_client.models.utils import get_or_create
from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import AccessToken, MultiAuth, TokenVerifier
from fastmcp.server.auth.providers.github import GitHubProvider
from fastmcp.server.middleware import Middleware, MiddlewareContext
from httpx import Timeout as HttpxTimeout
from mcp.types import ImageContent, TextContent
from pydantic import Field

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("bsky-mcp")

load_dotenv()

_TZ_LOCAL = ZoneInfo("America/Toronto")

_POST_ID_TTL = 3600  # seconds — rolling window for get_post_by_id
# An expired ID stops resolving after _POST_ID_TTL, but its number is not handed
# to a different post until this much later, so a stale ID in an old chat turn
# fails loudly instead of silently naming some other post.
_POST_ID_REUSE_AFTER = max(_POST_ID_TTL, int(float(os.getenv("MCP_POST_ID_REUSE_HOURS", "24")) * 3600))


# Read here, validated at startup (_require_credentials): importing the module
# must not need credentials or the network, so tests can import it.
BSKY_HANDLE = os.getenv("BSKY_HANDLE", "")
BSKY_APP_PASSWORD = os.getenv("BSKY_APP_PASSWORD", "")
# PDS / entryway base URL. Accounts hosted on a non-Bluesky PDS must point
# this at their own PDS, since both the SDK client and the raw XRPC calls
# below target this host.
BSKY_SERVICE = os.getenv("BSKY_SERVICE", "https://bsky.social").rstrip("/")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8765"))
# Auth (see _build_auth): a static bearer token for CLIs and bots, sent as
# `Authorization: Bearer <token>`, and/or GitHub OAuth for connector clients
# (claude.ai, Claude apps) that cannot send custom headers. The endpoint path
# no longer carries a secret: URL tokens end up in access logs.
MCP_AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN", "")
MCP_PUBLIC_URL = os.getenv("MCP_PUBLIC_URL", "").rstrip("/")
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")
# Numeric GitHub user IDs (stable across username changes), comma-separated.
MCP_ALLOWED_GITHUB_IDS = {s.strip() for s in os.getenv("MCP_ALLOWED_GITHUB_IDS", "").split(",") if s.strip()}
MCP_PATH = "/mcp"

# --- Bluesky client ---
#
# Nothing here touches the network at import time: _startup() logs in when the
# server runs, and the _SessionGuard middleware covers a login that failed at
# startup. That keeps a Bluesky outage from crash-looping
# the service and lets tests import this module.

client = Client(BSKY_SERVICE, request=AtprotoRequest(
    timeout=HttpxTimeout(connect=5.0, read=15.0, write=10.0, pool=5.0)
))

# One lock for every session transition — the SDK's own refresh, our raw-XRPC
# refresh, logins, and the chat-proxy clones. The SDK gives each clone its own
# _refresh_lock while they all share one session, so concurrent calls could
# otherwise refresh the same session twice. Reentrant because login() fetches
# the profile through _invoke, which takes the lock again.
_session_lock = threading.RLock()
client._refresh_lock = _session_lock

# Refresh tokens outlive restarts (~2 months); reusing one avoids a fresh
# createSession, which is rate limited per account, every time the service
# restarts. Empty disables persistence.
BSKY_SESSION_FILE = os.getenv("BSKY_SESSION_FILE", "bsky_session.txt")

_LOGIN_MAX_BACKOFF = 300.0
_login_state = {"retry_at": 0.0, "backoff": 0.0, "error": ""}

# Persistent HTTP client for raw XRPC calls — reuses TCP/TLS connections
_http = httpx.Client(timeout=30, headers={"User-Agent": "bsky-mcp/1.0"})


def _require_credentials() -> None:
    missing = [n for n, v in (("BSKY_HANDLE", BSKY_HANDLE), ("BSKY_APP_PASSWORD", BSKY_APP_PASSWORD)) if not v]
    if missing:
        raise SystemExit(f"Set {', '.join(missing)} in .env")


def _persist_session(_event, session) -> None:
    if not BSKY_SESSION_FILE:
        return
    try:
        tmp = BSKY_SESSION_FILE + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(session.export())
        os.replace(tmp, BSKY_SESSION_FILE)
    except Exception as exc:
        log.warning("Could not save the Bluesky session: %s", exc)


client.on_session_change(_persist_session)


def _read_saved_session() -> str | None:
    if not BSKY_SESSION_FILE:
        return None
    try:
        with open(BSKY_SESSION_FILE, encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def _session_is_dead(exc: Exception) -> bool:
    """True when the error proves the session itself is unusable.

    Only these justify a fresh login. Timeouts, 5xx and network failures are
    the service's problem, and answering them with createSession would turn an
    outage into a stream of rate-limited logins while hiding the real fault.
    """
    if isinstance(exc, (atproto_exc.LoginRequiredError, atproto_exc.UnauthorizedError)):
        return True
    if isinstance(exc, atproto_exc.BadRequestError):
        content = getattr(getattr(exc, "response", None), "content", None)
        return getattr(content, "error", None) in ("ExpiredToken", "InvalidToken")
    return False


def _login_locked() -> None:
    """Password login, with backoff after a failure. Caller holds _session_lock."""
    now = time.monotonic()
    if now < _login_state["retry_at"]:
        raise ToolError(
            f"Bluesky login failed recently ({_login_state['error']}); "
            f"next attempt in {int(_login_state['retry_at'] - now) + 1}s."
        )
    try:
        client.login(BSKY_HANDLE, BSKY_APP_PASSWORD)
    except Exception as exc:
        backoff = min(max(_login_state["backoff"] * 2, 5.0), _LOGIN_MAX_BACKOFF)
        _login_state.update(retry_at=now + backoff, backoff=backoff, error=_brief_error(exc))
        raise
    _login_state.update(retry_at=0.0, backoff=0.0, error="")
    log.info("Logged in to Bluesky as %s", BSKY_HANDLE)


def _sign_in_locked() -> None:
    """Resume the saved session if it still refreshes, else log in."""
    saved = _read_saved_session()
    if saved:
        try:
            client.login(session_string=saved)
            if (client._session.handle or "").lower() == BSKY_HANDLE.lower():
                log.info("Resumed saved Bluesky session for %s", BSKY_HANDLE)
                return
            log.info("Saved session belongs to another account; logging in")
        except Exception as exc:
            if not _session_is_dead(exc):
                raise
            log.info("Saved session is no longer valid (%s); logging in", _brief_error(exc))
    _login_locked()


def _ensure_logged_in() -> None:
    """Sign in if no session exists yet (startup login failed or never ran)."""
    if client.me is not None:
        return
    with _session_lock:
        if client.me is None:
            _sign_in_locked()


def _ensure_session():
    """Make sure the access token is usable for raw XRPC and chat calls.

    Uses the SDK's own refresh policy (15 minutes before expiry) under the
    shared session lock, so it never races the SDK's proactive refresh. A
    rejected refresh token means the session is dead and is replaced by a
    login; any other failure propagates for the caller's retry logic.
    """
    with _session_lock:
        if client.me is None or client._session is None:
            _sign_in_locked()
            return
        try:
            if client._should_refresh_session():
                client._refresh_and_set_session()
        except Exception as exc:
            if not _session_is_dead(exc):
                raise
            log.warning("Session refresh rejected (%s); logging in again", _brief_error(exc))
            _login_locked()


def _xrpc_get(endpoint: str, params: dict | None = None) -> dict:
    _ensure_session()
    token = client._session.access_jwt
    resp = _http.get(
        f"{BSKY_SERVICE}/xrpc/{endpoint}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()


def _xrpc_post(endpoint: str, body: dict) -> dict:
    _ensure_session()
    token = client._session.access_jwt
    resp = _http.post(
        f"{BSKY_SERVICE}/xrpc/{endpoint}",
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    if not resp.content or not resp.text.strip():
        return {}
    return resp.json()


def _dm():
    """Return the Bluesky chat (DM) convo namespace on a session-fresh client.

    Bluesky DMs live behind the central chat service, not the account's PDS, so
    requests must carry the `atproto-proxy: did:web:api.bsky.chat#bsky_chat`
    header. `with_bsky_chat_proxy()` clones the live client (sharing its
    session) and injects that header; the clone is handed the shared session
    lock so its refreshes serialize with everyone else's. Group chats are just
    convos with kind="group", so the same namespace handles 1:1 and group
    messages.

    NOTE: the account's app password must have "Allow access to your direct
    messages" enabled, or every call raises InvalidToken ("Bad token method").
    """
    _ensure_session()
    chat = client.with_bsky_chat_proxy()
    chat._refresh_lock = _session_lock
    return chat.chat.bsky.convo


def _get_raw_preferences() -> list[dict]:
    """Fetch preferences via raw XRPC call to avoid SDK validation errors on newer pref types."""
    return _xrpc_get("app.bsky.actor.getPreferences").get("preferences", [])


def _extract_saved_feed_uris(prefs: list[dict]) -> list[str]:
    """Extract feed generator URIs from raw preferences data."""
    generator_uris = []
    for pref in prefs:
        ptype = pref.get("$type", "")
        if ptype == "app.bsky.actor.defs#savedFeedsPref":
            generator_uris.extend(
                u for u in pref.get("saved", []) if "app.bsky.feed.generator" in u
            )
            generator_uris.extend(
                u for u in pref.get("pinned", []) if "app.bsky.feed.generator" in u
            )
        elif ptype == "app.bsky.actor.defs#savedFeedsPrefV2":
            for item in pref.get("items", []):
                value = item.get("value", "")
                if "app.bsky.feed.generator" in value:
                    generator_uris.append(value)
    return list(dict.fromkeys(generator_uris))


# --- Caches ---
#
# name.lower() -> AT URI. A refresh builds a new dict and swaps it in whole, so
# a failed refresh keeps the last good snapshot and readers never see a
# half-built map. The generation counters let a burst of misses share one
# refresh instead of each issuing its own.

feed_cache: dict[str, str] = {}
list_cache: dict[str, str] = {}
_cache_gen = {"feed": 0, "list": 0}
_cache_refresh_locks = {"feed": threading.Lock(), "list": threading.Lock()}
_cache_write_lock = threading.Lock()


def _unique_cache_key(cache: dict, name: str, uri: str) -> str:
    """A key for this URI: the bare name, or the name plus a URI-derived suffix
    when a different URI already holds it.

    The suffix starts with the rkey (readable) and falls back to the rkey plus
    the repo DID tail (rkeys repeat across accounts), then a counter, checking
    each candidate so two sources can never share a key.
    """
    key = name.lower()
    if cache.get(key, uri) == uri:
        return key
    parts = uri.removeprefix("at://").split("/")
    repo = parts[0] if parts else ""
    rkey = parts[-1] if len(parts) > 1 else uri
    candidates = [f"{key} ({rkey})", f"{key} ({rkey} {repo[-8:]})"]
    candidates += (f"{key} ({rkey} {repo[-8:]} #{n})" for n in range(2, 1000))
    for candidate in candidates:
        if cache.get(candidate, uri) == uri:
            return candidate
    raise RuntimeError(f"could not find a unique cache key for {uri}")


def _publish_cache(kind: str, new: dict[str, str]) -> None:
    global feed_cache, list_cache
    with _cache_write_lock:
        if kind == "feed":
            feed_cache = new
        else:
            list_cache = new
        _cache_gen[kind] += 1


def refresh_feed_cache() -> list[tuple[str, object]]:
    """Rebuild the feed cache from saved preferences. Returns (cache_key, generator_view) pairs."""
    prefs = _get_raw_preferences()
    generator_uris = _extract_saved_feed_uris(prefs)
    new: dict[str, str] = {}
    entries: list[tuple[str, object]] = []
    for i in range(0, len(generator_uris), 25):
        batch = generator_uris[i : i + 25]
        resp = client.app.bsky.feed.get_feed_generators({"feeds": batch})
        for gen in resp.feeds:
            key = _unique_cache_key(new, gen.display_name or gen.uri, gen.uri)
            new[key] = gen.uri
            entries.append((key, gen))
    _publish_cache("feed", new)
    return entries


def refresh_list_cache() -> list[tuple[str, object]]:
    """Rebuild list cache from account's lists. Returns (cache_key, list_view) pairs."""
    new: dict[str, str] = {}
    entries: list[tuple[str, object]] = []
    cursor = None
    while True:
        params = {"actor": BSKY_HANDLE, "limit": 50}
        if cursor:
            params["cursor"] = cursor
        resp = client.app.bsky.graph.get_lists(params)
        for lst in resp.lists:
            key = _unique_cache_key(new, lst.name, lst.uri)
            new[key] = lst.uri
            entries.append((key, lst))
        if not resp.cursor or resp.cursor == cursor:
            break
        cursor = resp.cursor
    _publish_cache("list", new)
    return entries


def _refresh_on_miss(kind: str, seen_gen: int) -> None:
    """Refresh one cache unless another call already did since `seen_gen`."""
    with _cache_refresh_locks[kind]:
        if _cache_gen[kind] == seen_gen:
            (refresh_feed_cache if kind == "feed" else refresh_list_cache)()


def refresh_caches():
    refresh_feed_cache()
    refresh_list_cache()


# --- SQLite ---
#
# sqlite3's own context manager commits or rolls back but never closes, so
# every use goes through _db(): closing() releases the connection, `with conn`
# keeps the transaction semantics. Schemas are created on first use, not at
# import.

_DB_PATH = "post_ids.db"
_MUTUALS_DB_PATH = "mutuals.db"

_DB_SCHEMAS = {
    _DB_PATH: (
        """CREATE TABLE IF NOT EXISTS posts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            at_uri     TEXT NOT NULL,
            fetched_at REAL NOT NULL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_fetched_at ON posts(fetched_at)",
        "CREATE INDEX IF NOT EXISTS idx_at_uri ON posts(at_uri)",
        # Posts published by create_post/create_thread, so a call retried after
        # a client timeout resumes instead of publishing a duplicate.
        """CREATE TABLE IF NOT EXISTS publish_journal (
            op_key     TEXT NOT NULL,
            idx        INTEGER NOT NULL,
            uri        TEXT NOT NULL,
            cid        TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (op_key, idx)
        )""",
    ),
    # A hand-curated set of accounts that matter to the user (see the mutuals
    # section). Its own database, not a table in post_ids.db: that one is
    # scratch that is safe to delete at any time, and this is durable state.
    _MUTUALS_DB_PATH: (
        """CREATE TABLE IF NOT EXISTS mutuals (
            did      TEXT PRIMARY KEY,
            handle   TEXT NOT NULL,
            note     TEXT,
            added_at REAL NOT NULL
        )""",
    ),
}
_db_ready: set[str] = set()
_db_ready_lock = threading.Lock()


@contextmanager
def _db(path: str):
    with closing(sqlite3.connect(path, timeout=10)) as conn:
        if path not in _db_ready:
            with _db_ready_lock:
                if path not in _db_ready:
                    with conn:
                        for stmt in _DB_SCHEMAS.get(path, ()):
                            conn.execute(stmt)
                    _db_ready.add(path)
        with conn:
            yield conn


def _db_store_posts(at_uris: list[str]) -> list[int]:
    """Return short IDs, refreshing repeats and recycling IDs after the reuse delay.

    An ID resolves for _POST_ID_TTL after it was last issued; its number stays
    reserved until _POST_ID_REUSE_AFTER, and a post seen again inside that span
    gets its old number back.
    """
    now = time.time()
    with _db(_DB_PATH) as conn:
        # Serialize allocation so two concurrent renders cannot choose the same
        # newly freed ID.  AUTOINCREMENT remains in the schema for compatibility
        # with existing databases, but IDs are assigned explicitly from the pool.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM posts WHERE fetched_at < ?", (now - _POST_ID_REUSE_AFTER,))

        used_ids = {row[0] for row in conn.execute("SELECT id FROM posts")}
        wanted = list(dict.fromkeys(at_uris))
        uri_to_id: dict[str, int] = {}
        for start in range(0, len(wanted), 500):
            chunk = wanted[start : start + 500]
            marks = ",".join("?" * len(chunk))
            # ORDER BY id: if a URI has several rows from the old allocator, the
            # newest one wins for future renders; the others age out.
            for post_id, uri in conn.execute(
                f"SELECT id, at_uri FROM posts WHERE at_uri IN ({marks}) ORDER BY id", chunk
            ):
                uri_to_id[uri] = post_id

        next_id = 1
        ids: list[int] = []
        for uri in at_uris:
            post_id = uri_to_id.get(uri)
            if post_id is not None:
                # Seeing the post again renews this ID's lease.
                conn.execute(
                    "UPDATE posts SET fetched_at = ? WHERE id = ?", (now, post_id)
                )
            else:
                while next_id in used_ids:
                    next_id += 1
                post_id = next_id
                conn.execute(
                    "INSERT INTO posts (id, at_uri, fetched_at) VALUES (?, ?, ?)",
                    (post_id, uri, now),
                )
                used_ids.add(post_id)
                uri_to_id[uri] = post_id
                next_id += 1
            ids.append(post_id)
    return ids


def _db_lookup_post(post_id: int) -> str | None:
    """Return the AT URI for a post ID if it is still within the TTL window."""
    cutoff = time.time() - _POST_ID_TTL
    with _db(_DB_PATH) as conn:
        row = conn.execute(
            "SELECT at_uri FROM posts WHERE id = ? AND fetched_at >= ?",
            (post_id, cutoff),
        ).fetchone()
    return row[0] if row else None


_PUBLISH_JOURNAL_TTL = 3600


def _journal_lookup(op_key: str) -> dict[int, tuple[str, str]]:
    """Posts already published under this operation key: idx -> (uri, cid)."""
    cutoff = time.time() - _PUBLISH_JOURNAL_TTL
    with _db(_DB_PATH) as conn:
        conn.execute("DELETE FROM publish_journal WHERE created_at < ?", (cutoff,))
        rows = conn.execute(
            "SELECT idx, uri, cid FROM publish_journal WHERE op_key = ?", (op_key,)
        ).fetchall()
    return {idx: (uri, cid) for idx, uri, cid in rows}


def _journal_record(op_key: str, idx: int, uri: str, cid: str) -> None:
    try:
        with _db(_DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO publish_journal (op_key, idx, uri, cid, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (op_key, idx, uri, cid, time.time()),
            )
    except Exception as exc:  # the post exists either way; only resume suffers
        log.warning("Could not journal published post %s: %s", uri, exc)


def _journal_forget(op_key: str, from_idx: int = 0) -> None:
    with _db(_DB_PATH) as conn:
        conn.execute(
            "DELETE FROM publish_journal WHERE op_key = ? AND idx >= ?", (op_key, from_idx)
        )


# --- Mutuals roster ---
#
# A hand-curated set of accounts that matter to the user. Feed output names the
# ones present in the fetched window, which is how the reader tells them apart
# from the crowd — and the only way to find them at all in mode="index", whose
# entries are topics, not authors.
#
# Stored in mutuals.db (schema in _DB_SCHEMAS), durable unlike post_ids.db.


def _db_add_mutual(did: str, handle: str, note: str | None) -> bool:
    """Upsert one mutual. Returns True if it was new.

    Keyed on DID so a handle change updates in place instead of orphaning the
    entry; an existing note survives an add that doesn't supply one.
    """
    with _db(_MUTUALS_DB_PATH) as conn:
        existing = conn.execute(
            "SELECT note FROM mutuals WHERE did = ?", (did,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE mutuals SET handle = ?, note = COALESCE(?, note) WHERE did = ?",
                (handle, note, did),
            )
        else:
            conn.execute(
                "INSERT INTO mutuals (did, handle, note, added_at) VALUES (?, ?, ?, ?)",
                (did, handle, note, time.time()),
            )
    return existing is None


def _db_list_mutuals() -> list[tuple[str, str, str | None, float]]:
    """All mutuals as (did, handle, note, added_at), oldest first."""
    with _db(_MUTUALS_DB_PATH) as conn:
        return conn.execute(
            "SELECT did, handle, note, added_at FROM mutuals ORDER BY added_at"
        ).fetchall()


def _db_remove_mutual(handle_or_did: str) -> str | None:
    """Delete by DID or handle. Returns the removed handle, or None if absent."""
    key = handle_or_did.strip().lstrip("@").lower()
    with _db(_MUTUALS_DB_PATH) as conn:
        row = conn.execute(
            "SELECT did, handle FROM mutuals WHERE did = ? OR lower(handle) = ?",
            (handle_or_did.strip(), key),
        ).fetchone()
        if not row:
            return None
        conn.execute("DELETE FROM mutuals WHERE did = ?", (row[0],))
    return row[1]


def _mutuals_present_handles(did_handle_pairs) -> list[str]:
    """Roster members that appear among the given (did, handle) pairs.

    Matched on DID first — a mutual who changed handle since being added is
    still recognised — with a handle fallback for anything lacking a DID.
    """
    roster = _db_list_mutuals()
    if not roster:
        return []
    by_did = {did: handle for did, handle, _, _ in roster}
    by_handle = {handle.lower(): handle for _, handle, _, _ in roster}

    present: dict[str, None] = {}
    for did, handle in did_handle_pairs:
        stored = by_did.get(did) or by_handle.get((handle or "").lower())
        if stored:
            # Prefer the handle as currently seen; the roster's copy may be stale.
            present[handle or stored] = None
    return list(present)


def _mutuals_line(posts) -> str:
    """'mutuals here: ...' for a batch of PostViews, or '' when none match."""
    handles = _mutuals_present_handles(
        (getattr(getattr(p, "author", None), "did", "") or "",
         getattr(getattr(p, "author", None), "handle", "") or "")
        for p in posts
    )
    return "mutuals here: " + " ".join(handles) if handles else ""


# --- Constants ---

SEP_POST = "---"
VIRAL_THRESHOLD = 100
URL_TEMPLATE_NOTE = "[URLs: bsky.app/profile/{handle}/post/{rkey}]"
NotificationReason = Literal[
    "all",
    "like",
    "repost",
    "follow",
    "mention",
    "reply",
    "quote",
    "starterpack-joined",
    "verified",
    "unverified",
    "like-via-repost",
    "repost-via-repost",
    "subscribed-post",
    "contact-match",
]
_NOTIFICATION_REASONS = {
    "like",
    "repost",
    "follow",
    "mention",
    "reply",
    "quote",
    "starterpack-joined",
    "verified",
    "unverified",
    "like-via-repost",
    "repost-via-repost",
    "subscribed-post",
    "contact-match",
}


# --- Helpers ---


_BSKY_WEB_HOSTS = {"bsky.app", "www.bsky.app"}
_RKEY_RE = re.compile(r"^[A-Za-z0-9._:~-]{1,512}$")


def _parse_bsky_post_url(url: str) -> tuple[str, str]:
    """(handle-or-DID, rkey) from a bsky.app post URL.

    Parsed structurally: share links carry ?ref_src=… or #fragments, which a
    path regex would swallow into the rkey and turn into an invalid AT URI.
    """
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https") or (parsed.hostname or "").lower() not in _BSKY_WEB_HOSTS:
        raise ValueError(f"Invalid bsky.app URL: {url}")
    segments = [s for s in parsed.path.split("/") if s]
    # Trailing segments (/liked-by, /reposted-by, /quotes) still name the post.
    if len(segments) < 4 or segments[0] != "profile" or segments[2] != "post":
        raise ValueError(f"Invalid bsky.app post URL (expected /profile/<handle>/post/<rkey>): {url}")
    actor, rkey = segments[1], segments[3]
    if not _RKEY_RE.match(rkey):
        raise ValueError(f"Invalid post key in URL: {url}")
    return actor, rkey


def resolve_bsky_url(url: str) -> str:
    """Convert a https://bsky.app/profile/handle/post/rkey URL to an AT URI."""
    actor, rkey = _parse_bsky_post_url(url)
    did = actor if actor.startswith("did:") else _resolve_did(actor)
    return f"at://{did}/app.bsky.feed.post/{rkey}"


def normalize_uri(uri: str) -> str:
    """Accept either an AT URI or a bsky.app URL and return an AT URI."""
    if uri.startswith("at://"):
        return uri
    return resolve_bsky_url(uri)


def _thread_params(uri: str, depth: int, parent_height: int) -> dict:
    """Build validated getPostThread params."""
    if not 0 <= depth <= 1000:
        raise ValueError("depth must be between 0 and 1000.")
    if not 0 <= parent_height <= 1000:
        raise ValueError("parent_height must be between 0 and 1000.")
    return {"uri": uri, "depth": depth, "parentHeight": parent_height}


def _normalize_notification_reasons(reasons: list[NotificationReason] | None) -> list[str] | None:
    """Validate notification reasons. None means all notification reasons."""
    if not reasons:
        return None

    normalized: list[str] = []
    for reason in reasons:
        value = str(reason).strip().lower()
        if value == "all":
            return None
        if value not in _NOTIFICATION_REASONS:
            allowed = ", ".join(["all", *sorted(_NOTIFICATION_REASONS)])
            raise ValueError(f"Unsupported notification reason '{reason}'. Use one or more of: {allowed}")
        if value not in normalized:
            normalized.append(value)

    return normalized or None


def _root_uri_from_thread_node(node) -> str:
    """Return the topmost parent URI from a thread response node."""
    root = node
    while hasattr(root, "parent") and root.parent and hasattr(root.parent, "post"):
        root = root.parent
    if not hasattr(root, "post") or not getattr(root.post, "uri", None):
        raise ValueError("Could not resolve the root post for this thread.")
    return root.post.uri


def _cache_lookup(kind: str, key: str) -> str | None:
    """Look a name up in one cache, refreshing it once on a miss."""
    def current():
        return feed_cache if kind == "feed" else list_cache
    seen_gen = _cache_gen[kind]
    uri = current().get(key)
    if uri is None:
        _refresh_on_miss(kind, seen_gen)
        uri = current().get(key)
    return uri


def resolve_feed_uri(name_or_uri: str) -> str:
    if name_or_uri.startswith("at://"):
        return name_or_uri
    uri = _cache_lookup("feed", name_or_uri.lower())
    if uri is None:
        available = ", ".join(sorted(feed_cache)) or "(none)"
        raise ValueError(f"Feed '{name_or_uri}' not found. Available feeds: {available}")
    return uri


def resolve_list_uri(name_or_uri: str) -> str:
    if name_or_uri.startswith("at://"):
        return name_or_uri
    uri = _cache_lookup("list", name_or_uri.lower())
    if uri is None:
        available = ", ".join(sorted(list_cache)) or "(none)"
        raise ValueError(f"List '{name_or_uri}' not found. Available lists: {available}")
    return uri


_LIST_COLLECTION = "app.bsky.graph.list"
_LISTITEM_COLLECTION = "app.bsky.graph.listitem"
_LIST_PURPOSES = {
    "curation": "app.bsky.graph.defs#curatelist",
    "moderation": "app.bsky.graph.defs#modlist",
    "reference": "app.bsky.graph.defs#referencelist",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _record_uri_parts(uri: str, expected_collection: str | None = None) -> tuple[str, str, str]:
    """Split an AT record URI into (repo, collection, rkey), with validation."""
    if not uri.startswith("at://"):
        raise ValueError(f"Expected an at:// record URI, got: {uri}")
    parts = uri[5:].split("/")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"Expected an AT record URI with repo/collection/rkey: {uri}")
    repo, collection, rkey = parts
    if expected_collection and collection != expected_collection:
        raise ValueError(
            f"Expected a {expected_collection} URI, got collection {collection}: {uri}"
        )
    return repo, collection, rkey


def _owned_list_record(name_or_uri: str) -> tuple[str, str, dict]:
    """Resolve an owned list and return (uri, rkey, raw getRecord response)."""
    uri = resolve_list_uri(name_or_uri)
    repo, _collection, rkey = _record_uri_parts(uri, _LIST_COLLECTION)
    if repo != client.me.did:
        raise ValueError(
            "List writes are only allowed for lists owned by the authenticated account."
        )
    data = _xrpc_get(
        "com.atproto.repo.getRecord",
        {"repo": repo, "collection": _LIST_COLLECTION, "rkey": rkey},
    )
    return uri, rkey, data


def _cache_owned_list(name: str, uri: str) -> str:
    """Replace this URI's old cache key and return its current disambiguated key."""
    global list_cache
    with _cache_write_lock:
        new = {k: v for k, v in list_cache.items() if v != uri}
        key = _unique_cache_key(new, name, uri)
        new[key] = uri
        list_cache = new
    return key


def _remove_cached_list(uri: str) -> None:
    global list_cache
    with _cache_write_lock:
        list_cache = {k: v for k, v in list_cache.items() if v != uri}


# Mutations need the whole membership set; this only guards against a server
# that never stops paginating.
_REPO_SCAN_HARD_CAP = 1_000_000


def _raw_repo_records(collection: str, cap: int | None = None) -> tuple[list[dict], bool]:
    """Enumerate records in the authenticated repo, newest first.

    Returns (records, complete). complete is False when `cap` (or the hard cap)
    stopped the scan before the collection ended — callers that act on the
    absence of a record must treat that as unknown, not as absent.
    """
    limit = min(cap, _REPO_SCAN_HARD_CAP) if cap is not None else _REPO_SCAN_HARD_CAP
    records: list[dict] = []
    cursor = None
    previous = None
    while len(records) < limit:
        _raise_if_cancelled()
        params: dict = {
            "repo": client.me.did,
            "collection": collection,
            "limit": min(100, limit - len(records)),
        }
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get("com.atproto.repo.listRecords", params)
        page = data.get("records") or []
        if not page:
            return records, True
        records.extend(page)
        cursor = data.get("cursor")
        if not cursor:
            return records, True
        if cursor == previous:
            return records, False  # a repeating cursor proves nothing about the rest
        previous = cursor
    return records, False


def _listitem_index() -> dict[tuple[str, str], list[str]]:
    """Map (list URI, subject DID) to every underlying membership record URI.

    Covers every list in the account, so it must be complete: list writes act
    on what it does not contain (add skips existing members, delete removes
    exactly what it found).
    """
    items, complete = _raw_repo_records(_LISTITEM_COLLECTION)
    if not complete:
        raise ToolError(
            f"Could not enumerate every list membership record (stopped after {len(items)}); "
            "nothing was changed."
        )
    index: dict[tuple[str, str], list[str]] = {}
    for item in items:
        value = item.get("value") or {}
        list_uri = value.get("list")
        subject = value.get("subject")
        uri = item.get("uri")
        if list_uri and subject and uri:
            index.setdefault((list_uri, subject), []).append(uri)
    return index


def _apply_writes_in_chunks(writes: list[dict], chunk_size: int = 100) -> tuple[int, str | None]:
    """Apply atomic chunks and report the committed prefix if a later chunk fails."""
    applied = 0
    for start in range(0, len(writes), chunk_size):
        if _cancelled():
            log.warning("applyWrites cancelled by the client after %d/%d writes", applied, len(writes))
            return applied, "cancelled by the client"
        chunk = writes[start : start + chunk_size]
        try:
            _xrpc_post(
                "com.atproto.repo.applyWrites",
                {"repo": client.me.did, "validate": True, "writes": chunk},
            )
        except Exception as exc:
            return applied, _brief_error(exc)
        applied += len(chunk)
    return applied, None


def _model_json(value):
    """Convert an atproto/Pydantic model into alias-preserving JSON data."""
    if hasattr(value, "model_dump"):
        return value.model_dump(by_alias=True, exclude_none=True)
    if hasattr(value, "dict"):
        return value.dict(by_alias=True, exclude_none=True)
    return value


def _resolve_actor_inputs(actors: list[str] | str) -> tuple[list[dict], list[str], list[str]]:
    """Resolve actor inputs efficiently; return resolved, failed, duplicate inputs."""
    wanted = _v2_list(actors, strip_prefix="@") or []
    if not wanted:
        return [], [], []

    profiles: dict[str, dict] = {}
    for start in range(0, len(wanted), 25):
        batch = wanted[start : start + 25]
        try:
            data = _xrpc_get("app.bsky.actor.getProfiles", {"actors": batch})
        except Exception:
            data = {"profiles": []}
        for profile in data.get("profiles") or []:
            did = profile.get("did") or ""
            handle = profile.get("handle") or ""
            if did:
                profiles[did] = profile
            if handle:
                profiles[handle.lower()] = profile

    resolved: list[dict] = []
    failed: list[str] = []
    duplicates: list[str] = []
    seen_dids: set[str] = set()
    for original in wanted:
        key = original if original.startswith("did:") else original.lower()
        profile = profiles.get(key)
        if profile is None:
            try:
                did = _resolve_did(original)
                raw = client.app.bsky.actor.get_profile({"actor": did})
                profile = {
                    "did": raw.did,
                    "handle": raw.handle,
                    "displayName": getattr(raw, "display_name", None),
                }
            except Exception as exc:
                failed.append(f"{original} ({_brief_error(exc)})")
                continue
        did = profile.get("did") or ""
        if not did:
            failed.append(f"{original} (profile had no DID)")
            continue
        if did in seen_dids:
            duplicates.append(original)
            continue
        seen_dids.add(did)
        resolved.append(
            {
                "input": original,
                "did": did,
                "handle": profile.get("handle") or did,
                "display_name": profile.get("displayName") or "",
            }
        )
    return resolved, failed, duplicates


def _resolve_removal_inputs(actors: list[str] | str) -> tuple[list[dict], list[str], list[str]]:
    """Like _resolve_actor_inputs, but without requiring a live profile.

    Removal is exactly when an account may be deactivated, suspended or
    deleted, so a DID is used as given and a handle only needs to resolve to a
    DID; the stored membership index decides what exists.
    """
    wanted = _v2_list(actors, strip_prefix="@") or []
    resolved: list[dict] = []
    failed: list[str] = []
    duplicates: list[str] = []
    seen_dids: set[str] = set()
    for original in wanted:
        try:
            did = _resolve_did(original)
        except Exception as exc:
            failed.append(f"{original} ({_brief_error(exc)})")
            continue
        if did in seen_dids:
            duplicates.append(original)
            continue
        seen_dids.add(did)
        resolved.append({"input": original, "did": did, "handle": original, "display_name": ""})
    return resolved, failed, duplicates


# --- Timestamp helpers ---


def _parse_ts(raw) -> datetime | None:
    """Parse a timestamp string or datetime to a timezone-aware datetime."""
    if raw is None:
        return None
    try:
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if isinstance(raw, datetime):
            if raw.tzinfo is None:
                return raw.replace(tzinfo=timezone.utc)
            return raw
        return None
    except Exception:
        return None


def _get_feed_item_timestamp(feed_view) -> datetime | None:
    """Get the relevant timestamp for a feed item.

    For reposts, use reason.indexedAt (when the repost happened).
    For regular posts, use post.indexedAt.
    Falls back to record.created_at.
    """
    # Check for repost
    if hasattr(feed_view, "reason") and feed_view.reason:
        reason_type = getattr(feed_view.reason, "py_type", "") or ""
        if "reasonRepost" in reason_type:
            ts = _parse_ts(getattr(feed_view.reason, "indexed_at", None))
            if ts:
                return ts

    post = feed_view.post if hasattr(feed_view, "post") else feed_view
    ts = _parse_ts(getattr(post, "indexed_at", None))
    if ts:
        return ts
    record = getattr(post, "record", None)
    if record is None:
        return None
    return _parse_ts(getattr(record, "created_at", None))


def _fmt_ts(feed_view) -> str:
    """Format the feed item timestamp for display."""
    ts = _get_feed_item_timestamp(feed_view)
    if ts:
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    return "unknown time"


def _fmt_dt_compact(dt: datetime | None, since_minutes: int | None) -> str:
    """Compact datetime in America/Toronto time.

    HHMM inside a <24 h window; DD HHMM inside a bounded window of under
    28 days (the window bounds disambiguate the month); full YY-MM-DD HHMM
    when the window is unbounded or 28+ days (bookmarks, likes, count-mode
    fetches, quote-table entries) — those can span months or years, so a
    bare day-of-month is unreadable.
    """
    if not dt:
        return "?"
    local = dt.astimezone(_TZ_LOCAL)
    if since_minutes is not None and since_minutes < 1440:
        return local.strftime("%H%M")
    if since_minutes is not None and since_minutes < 40320:
        return local.strftime("%d %H%M")
    return local.strftime("%y-%m-%d %H%M")


def _fmt_ts_compact(feed_view, since_minutes: int | None) -> str:
    return _fmt_dt_compact(_get_feed_item_timestamp(feed_view), since_minutes)


# --- Text helpers ---


def _truncate(text, max_chars=300):
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ..."


def _sanitize_for_row(text: str) -> str:
    """Replace characters that would break the compact pipe-delimited row format.

    The schema uses ` | ` between fields and `\\n` between rows, so we substitute
    pipes with U+00A6 (broken bar) and collapse any line break to a single space.
    """
    if not text:
        return text
    return text.replace("|", "¦").replace("\r\n", " ").replace("\n", " ").replace("\r", " ")


def _get_text(record) -> str:
    if hasattr(record, "text"):
        return (record.text or "").strip()
    return ""


# --- Embed formatting (plain text, matching reference script) ---


def _get_gallery_items(embed_or_media) -> list:
    """Image items of an app.bsky.embed.gallery view, or [].

    Galleries (added to the lexicon mid-2026) carry alt/fullsize/thumbnail per
    item; we render them like multi-image embeds everywhere.
    """
    if embed_or_media is None:
        return []
    items = getattr(embed_or_media, "items", None)
    # Guard against dict-likes (DotDict), where .items is the mapping method.
    return list(items) if isinstance(items, (list, tuple)) else []


def _video_alt(container) -> str | None:
    """Alt text ("" if none) when `container` is a video embed, else None.

    The AppView returns app.bsky.embed.video#view, which exposes playlist,
    thumbnail and alt directly; only the authoring record nests a blob under
    .video (with alt beside it). Both shapes reach the renderers — views in
    feeds, records inside quoted-post values — so both are recognised.
    """
    if container is None:
        return None
    pt = (getattr(container, "py_type", "") or "").lower()
    if getattr(container, "playlist", None) or pt.startswith("app.bsky.embed.video"):
        return (getattr(container, "alt", "") or "").strip()
    video = getattr(container, "video", None)
    if video:
        return (getattr(container, "alt", None) or getattr(video, "alt", "") or "").strip()
    return None


def _get_embed_info(post, include_image_urls: bool = False) -> str | None:
    """Format embed info as a text line.

    include_image_urls appends each image's CDN URL so a multimodal client
    can fetch and look at otherwise unclassifiable image-only posts
    (single-post views only — too heavy for threads/notifications).
    """
    embed = getattr(post, "embed", None)
    if not embed:
        return None

    def _img_urls(imgs) -> str:
        if not include_image_urls:
            return ""
        urls = [
            getattr(i, "fullsize", None) or getattr(i, "thumb", None)
            or getattr(i, "thumbnail", "") or ""
            for i in imgs
        ]
        urls = [u for u in urls if u]
        return (" URLs: " + " ; ".join(urls)) if urls else ""

    # External link
    if hasattr(embed, "external") and embed.external:
        ext = embed.external
        parts = []
        title = getattr(ext, "title", "") or ""
        uri = getattr(ext, "uri", "") or ""
        desc = getattr(ext, "description", "") or ""
        if title:
            parts.append(f"Title: {title}")
        if uri:
            parts.append(f"URL: {uri}")
        if desc:
            parts.append(f"Desc: {_truncate(desc, 150)}")
        return "[Link] " + " | ".join(parts) if parts else "[Link]"

    # Images or gallery (not recordWithMedia)
    if not hasattr(embed, "media"):
        imgs = (embed.images if hasattr(embed, "images") and embed.images else None) \
            or _get_gallery_items(embed)
        if imgs:
            alts = [getattr(i, "alt", "").strip() for i in imgs if getattr(i, "alt", "").strip()]
            base = f"[{len(imgs)} image(s)]"
            if alts:
                base += " Alt: " + "; ".join(alts)
            return base + _img_urls(imgs)

    # Video
    alt = _video_alt(embed)
    if alt is not None:
        return "[Video]" + (f" Alt: {alt}" if alt else "")

    # Record with media
    if hasattr(embed, "media") and embed.media:
        media = embed.media
        if hasattr(media, "external") and media.external:
            ext = media.external
            parts = []
            title = getattr(ext, "title", "") or ""
            uri = getattr(ext, "uri", "") or ""
            desc = getattr(ext, "description", "") or ""
            if title:
                parts.append(f"Title: {title}")
            if uri:
                parts.append(f"URL: {uri}")
            if desc:
                parts.append(f"Desc: {_truncate(desc, 150)}")
            return "[Quote+link] " + " | ".join(parts) if parts else "[Quote+link]"
        media_imgs = (media.images if hasattr(media, "images") and media.images else None) \
            or _get_gallery_items(media)
        if media_imgs:
            imgs = media_imgs
            alts = [getattr(i, "alt", "").strip() for i in imgs if getattr(i, "alt", "").strip()]
            base = f"[Quote+{len(imgs)} image(s)]"
            if alts:
                base += " Alt: " + "; ".join(alts)
            return base + _img_urls(imgs)
        alt = _video_alt(media)
        if alt is not None:
            return "[Quote+video]" + (f" Alt: {alt}" if alt else "")
        return "[Quote+media]"

    return None


def _extract_quoted_post(post):
    """Extract quoted post info. Returns (handle, display, ts_datetime, text) or None.

    For viewNotFound / viewBlocked / viewDetached quotes, returns
    ("?", "?", None, "[Quoted post not available: <reason>]") so the marker
    surfaces in callers' text field.
    """
    embed = getattr(post, "embed", None)
    if not embed:
        return None

    # Find the record embed (direct quote or recordWithMedia)
    record_embed = None
    if hasattr(embed, "record") and embed.record:
        record_embed = embed.record

    if not record_embed:
        return None

    # The inner record (ViewRecord)
    inner = record_embed
    if hasattr(record_embed, "record"):
        inner = record_embed.record

    # Unavailable quotes: the discriminators are #viewNotFound, #viewBlocked
    # and #viewDetached, and the models also carry a matching boolean flag.
    inner_type = (getattr(inner, "py_type", "") or "").lower()
    for marker, flag, reason in (("notfound", "not_found", "deleted"),
                                 ("blocked", "blocked", "blocked"),
                                 ("detached", "detached", "detached by its author")):
        if marker in inner_type or getattr(inner, flag, False) is True:
            return ("?", "?", None, f"[Quoted post not available: {reason}]")

    if not hasattr(inner, "author"):
        return None

    author = inner.author
    handle = getattr(author, "handle", "?")
    display = getattr(author, "display_name", None) or handle

    # Get text
    text = ""
    if hasattr(inner, "value") and inner.value:
        text = _get_text(inner.value)
    elif hasattr(inner, "record") and inner.record:
        text = _get_text(inner.record)

    # Get timestamp (raw datetime — callers format to taste)
    ts_dt = None
    created = None
    if hasattr(inner, "value") and inner.value:
        created = getattr(inner.value, "created_at", None)
    elif hasattr(inner, "record") and inner.record:
        created = getattr(inner.record, "created_at", None)
    if created:
        ts_dt = _parse_ts(created)

    return (handle, display, ts_dt, text)


def _has_link_embed(post) -> bool:
    """Check if a post has an external link embed."""
    embed = getattr(post, "embed", None)
    if not embed:
        return False
    if hasattr(embed, "external") and embed.external:
        return True
    if hasattr(embed, "media") and embed.media:
        if hasattr(embed.media, "external") and embed.media.external:
            return True
    return False


def _effective_text_length(feed_view) -> int:
    """Return length of the parent post's own text only (excludes quoted post text and alt text)."""
    post = feed_view.post if hasattr(feed_view, "post") else feed_view
    record = getattr(post, "record", None)
    return len(_get_text(record)) if record else 0


def _passes_min_chars(feed_view, min_chars: int) -> bool:
    """Check if a post passes the minimum character filter.

    Exempt if the post has an external link embed.
    Otherwise, post text + quoted text must meet the threshold.
    """
    if min_chars <= 0:
        return True
    post = feed_view.post if hasattr(feed_view, "post") else feed_view
    if _has_link_embed(post):
        return True
    return _effective_text_length(feed_view) >= min_chars


def _get_embed_type(post) -> tuple[str, int]:
    """Return (embed_type, image_count) for scoring. embed_type: 'url'|'video'|'images'|'none'."""
    embed = getattr(post, "embed", None)
    if not embed:
        return "none", 0
    if hasattr(embed, "external") and embed.external:
        return "url", 0
    if _video_alt(embed) is not None:
        return "video", 0
    if hasattr(embed, "images") and embed.images and not hasattr(embed, "media"):
        return "images", len(embed.images)
    gallery = _get_gallery_items(embed) if not hasattr(embed, "media") else []
    if gallery:
        return "images", len(gallery)
    if hasattr(embed, "media") and embed.media:
        media = embed.media
        if hasattr(media, "external") and media.external:
            return "url", 0
        if hasattr(media, "images") and media.images:
            return "images", len(media.images)
        if _video_alt(media) is not None:
            return "video", 0
        media_gallery = _get_gallery_items(media)
        if media_gallery:
            return "images", len(media_gallery)
    return "none", 0


def _get_external_embed(post):
    """A post's external-link embed object (direct or recordWithMedia), or None."""
    embed = getattr(post, "embed", None)
    if not embed:
        return None
    if hasattr(embed, "external") and embed.external:
        return embed.external
    media = getattr(embed, "media", None)
    if media is not None and getattr(media, "external", None):
        return media.external
    return None


def _get_external_url(post) -> str | None:
    """URL of a post's external-link embed (direct or recordWithMedia)."""
    ext = _get_external_embed(post)
    return getattr(ext, "uri", None) if ext else None


def _get_external_meta(post) -> tuple[str, str, str]:
    """(uri, title, description) of a post's link card — all "" when absent.

    Title and description matter beyond display: Bluesky's search backend
    indexes them, so a post whose visible text names nothing is still
    retrievable through its card. The index builder needs the same fields the
    search engine has, or it emits queries that retrieve nothing.
    """
    ext = _get_external_embed(post)
    if not ext:
        return "", "", ""
    return (
        getattr(ext, "uri", "") or "",
        getattr(ext, "title", "") or "",
        getattr(ext, "description", "") or "",
    )


def _get_image_alts(post) -> list[str]:
    """Non-empty alt texts for a post's image embeds (direct or recordWithMedia)."""
    embed = getattr(post, "embed", None)
    if not embed:
        return []
    imgs = []
    if hasattr(embed, "images") and embed.images and not hasattr(embed, "media"):
        imgs = embed.images
    elif hasattr(embed, "media") and embed.media and getattr(embed.media, "images", None):
        imgs = embed.media.images
    else:
        imgs = (_get_gallery_items(embed) if not hasattr(embed, "media") else []) \
            or _get_gallery_items(getattr(embed, "media", None))
    return [a for a in ((getattr(i, "alt", "") or "").strip() for i in imgs) if a]


def _get_post_image_items(post) -> list:
    """Image view objects attached directly to a post, in display order.

    Handles legacy app.bsky.embed.images (up to 4), the newer gallery embed
    (currently a 10-item authoring soft limit / 20-item schema ceiling), and
    either form nested inside recordWithMedia. Images belonging to the quoted
    post itself are deliberately excluded: this returns media attached to the
    requested post, matching what _get_embed_info describes.
    """
    embed = getattr(post, "embed", None)
    if not embed:
        return []
    media = getattr(embed, "media", None)
    container = media if media is not None else embed
    images = getattr(container, "images", None)
    if isinstance(images, (list, tuple)):
        return list(images)
    return _get_gallery_items(container)


# --- Post formatter ---


def format_post_text(feed_view, reposters=None, reply_parent=None, image_urls: bool = False) -> str:
    """Format a single feed item as compact text."""
    if reposters is None:
        reposters = []

    post = feed_view.post if hasattr(feed_view, "post") else feed_view

    author = getattr(post, "author", None)
    if not author or not hasattr(author, "handle"):
        return ""

    record = post.record
    handle = author.handle
    display = author.display_name or handle
    ts = _fmt_ts(feed_view)
    rkey = post.uri.split("/")[-1] if post.uri else ""

    lines = []

    # Header: @handle (Display) | ts | rk:rkey
    header = f"@{handle}"
    if display != handle:
        header += f" ({display})"
    header += f" | {ts}"
    if rkey:
        header += f" | rk:{rkey}"
    lines.append(header)

    # Repost info — prefer deduplicated reposters list; fall back to feed reason
    if reposters:
        rp_handles = [f"@{by.get('handle', '?')}" for by in reposters]
        lines.append(f"Rp: {', '.join(rp_handles)}")
    elif hasattr(feed_view, "reason") and feed_view.reason:
        reason_type = getattr(feed_view.reason, "py_type", "") or ""
        if "reasonRepost" in reason_type:
            by = getattr(feed_view.reason, "by", None)
            rp_handle = getattr(by, "handle", None) if by else None
            if rp_handle:
                lines.append(f"Rp: @{rp_handle}")

    # Reply info
    if hasattr(record, "reply") and record.reply:
        reply_to_handle = None
        parent_text = None

        if reply_parent is not None:
            parent_obj = reply_parent.post if hasattr(reply_parent, "post") else reply_parent
            if hasattr(parent_obj, "author") and hasattr(parent_obj.author, "handle"):
                reply_to_handle = parent_obj.author.handle
            if hasattr(parent_obj, "record"):
                parent_text = _get_text(parent_obj.record)

        if not reply_to_handle and hasattr(feed_view, "parent") and feed_view.parent:
            parent_node = feed_view.parent
            parent_obj = parent_node.post if hasattr(parent_node, "post") else parent_node
            if hasattr(parent_obj, "author") and hasattr(parent_obj.author, "handle"):
                reply_to_handle = parent_obj.author.handle
            if hasattr(parent_obj, "record"):
                parent_text = _get_text(parent_obj.record)

        if not reply_to_handle and hasattr(feed_view, "reply") and feed_view.reply:
            fv_reply = feed_view.reply
            if hasattr(fv_reply, "parent") and fv_reply.parent:
                parent_obj = fv_reply.parent
                if hasattr(parent_obj, "author") and hasattr(parent_obj.author, "handle"):
                    reply_to_handle = parent_obj.author.handle
                if hasattr(parent_obj, "record"):
                    parent_text = _get_text(parent_obj.record)

        if reply_to_handle:
            re_line = f"Re: @{reply_to_handle}"
        else:
            parent_uri = ""
            if record.reply and hasattr(record.reply, "parent") and record.reply.parent:
                parent_uri = getattr(record.reply.parent, "uri", "")
            # AT URI: at://did:plc:xxx/app.bsky.feed.post/rkey — parts[2] is a DID,
            # not a handle, so don't render it with an @ prefix.
            authority = parent_uri.split("/")[2] if len(parent_uri.split("/")) > 2 else "unknown"
            re_line = f"Re: {authority}" if authority.startswith("did:") else f"Re: @{authority}"
        if parent_text:
            re_line += f' "{_truncate(parent_text, 200)}"'
        lines.append(re_line)

    # Post text
    text = _get_text(record)
    if text:
        lines.append(text)

    # Quoted post
    quoted = _extract_quoted_post(post)
    if quoted:
        q_handle, q_display, q_ts_dt, q_text = quoted
        qt_line = f"Qt: @{q_handle}"
        if q_ts_dt:
            qt_line += f" [{q_ts_dt.strftime('%H:%M UTC')}]"
        if q_text:
            qt_line += f' "{_truncate(q_text, 200)}"'
        lines.append(qt_line)

    # Embed info
    embed_info = _get_embed_info(post, include_image_urls=image_urls)
    if embed_info:
        lines.append(embed_info)

    # Engagement
    like_count = post.like_count or 0
    repost_count = post.repost_count or 0
    reply_count = post.reply_count or 0
    viral_flag = " [!]" if like_count >= VIRAL_THRESHOLD else ""
    lines.append(f"L:{like_count} Rp:{repost_count} Re:{reply_count}{viral_flag}")

    return "\n".join(lines)


# --- Cancellation ---
# asyncio.to_thread cannot stop a running worker: cancelling the awaiting task
# only abandons it, and a synchronous publisher would carry on through every
# remaining write. _run_tool hands the worker an Event through a ContextVar
# (to_thread copies the context into the thread) and sets it when the call is
# cancelled; workers check it between side effects and between pages.

_cancel_event: contextvars.ContextVar[threading.Event | None] = contextvars.ContextVar(
    "_cancel_event", default=None
)


class _Cancelled(Exception):
    """The client cancelled the tool call; nobody will read the result."""


def _cancelled() -> bool:
    event = _cancel_event.get()
    return event is not None and event.is_set()


def _raise_if_cancelled(progress: str = "") -> None:
    if _cancelled():
        if progress:
            log.warning("Tool call cancelled by the client; %s", progress)
        raise _Cancelled(progress or "cancelled by the client")


async def _run_tool(fn, *args):
    """asyncio.to_thread with cooperative cancellation (see above)."""
    event = threading.Event()
    token = _cancel_event.set(event)
    try:
        return await asyncio.to_thread(fn, *args)
    except asyncio.CancelledError:
        event.set()
        raise
    finally:
        _cancel_event.reset(token)


# --- Pagination ---

_TIME_FETCH_CAP = 20000  # hard cap on raw items held by one time-window fetch
_MAX_FEED_PAGES = 500
# How many consecutive pages entirely older than the window an unordered
# (ranked) source may return before the walk gives up — see fetch_by_time.
_RANKED_OLD_PAGES = 3
# Statuses worth retrying for a read. 409/413 come back from the SDK as
# NetworkError too, but replaying them changes nothing.
_TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


class _UpstreamError(Exception):
    """A read still failed after its retries; the message is a one-line reason."""


class _Fetched(list):
    """Feed items plus notes on how the walk ended, shown in the output header.

    A walk that stops on an upstream error, a page cap or a ranked-feed budget
    must say so — otherwise a partial window reads as the whole window.
    """

    def __init__(self, items=(), notes=None):
        super().__init__(items)
        self.notes: list[str] = list(notes or [])


def _error_status(exc) -> int | None:
    return getattr(getattr(exc, "response", None), "status_code", None)


def _is_transient(exc: Exception) -> bool:
    """Timeouts, transport failures, 429 and gateway/service 5xx.

    Decided on exception type and status code, never on the message: an SDK
    InvokeTimeoutError raised without a response stringifies to "".
    """
    status = _error_status(exc)
    if status is not None:
        return status in _TRANSIENT_STATUS
    return isinstance(exc, (atproto_exc.NetworkError, httpx.TransportError))


def _retry_delay(exc, fallback: float) -> float:
    """Server-requested wait for a 429 (capped at 60 s), else `fallback`."""
    wait = getattr(exc, "retry_after", None)  # RateLimitExceededError parses both forms
    if wait is None:
        raw = (getattr(getattr(exc, "response", None), "headers", None) or {}).get("retry-after")
        try:
            wait = float(raw) if raw else None
        except (TypeError, ValueError):
            wait = None  # an HTTP-date; use our own backoff
    return min(wait, 60.0) if wait is not None else fallback


def _retry_fetch(fetch_fn, cursor, limit: int = 100, max_attempts: int = 3):
    """Call fetch_fn(cursor=..., limit=...), retrying transient failures.

    Raises _UpstreamError once the retries are spent, so pagination can tell
    "the source ended" from "the source failed"; any other error propagates
    unchanged. Only page reads go through here, and those are safe to replay.
    """
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        _raise_if_cancelled()
        try:
            return fetch_fn(cursor=cursor, limit=limit)
        except Exception as e:
            if not _is_transient(e):
                raise
            status = _error_status(e)
            reason = f"HTTP {status}" if status else type(e).__name__
            if attempt == max_attempts:
                raise _UpstreamError(f"{reason} after {max_attempts} attempts") from e
            wait = _retry_delay(e, delay) if status == 429 else delay
            log.warning("Transient %s on cursor %s (attempt %d/%d), retrying in %.1fs",
                        reason, cursor, attempt, max_attempts, wait)
            time.sleep(wait)
            delay *= 2
    raise AssertionError("unreachable")


def _walk_failed(exc: _UpstreamError, usable: int, pages: int) -> str:
    """Note for a walk cut short by an upstream failure — or a ToolError if it got nothing."""
    if not usable:
        raise ToolError(f"Bluesky did not answer: {exc}. Nothing was fetched; try again shortly.") from exc
    return f"PARTIAL — upstream error on page {pages + 1} ({exc}); older posts are missing"


def fetch_by_count(fetch_fn, target_count: int, min_chars: int = 0) -> _Fetched:
    """Fetch raw feed items, paginating until we have target_count posts after dedup and filtering."""
    _t0 = time.monotonic()
    results = _Fetched()
    cursor = None
    seen_uris: set[str] = set()
    good_count = 0
    prev_cursor = None
    empty_pages = 0
    pages = 0
    while good_count < target_count:
        if pages >= _MAX_FEED_PAGES:
            results.notes.append(f"stopped after {_MAX_FEED_PAGES} pages")
            break
        try:
            resp = _retry_fetch(fetch_fn, cursor)
        except _UpstreamError as exc:
            results.notes.append(_walk_failed(exc, good_count, pages))
            break
        pages += 1
        feed = resp.feed if hasattr(resp, "feed") else []
        if not feed:
            empty_pages += 1
            if empty_pages >= 3:
                break
            cursor = resp.cursor if hasattr(resp, "cursor") else None
            if not cursor or cursor == prev_cursor:
                break
            prev_cursor = cursor
            continue
        empty_pages = 0
        done = False
        for item in feed:
            results.append(item)
            post = item.post if hasattr(item, "post") else item
            uri = post.uri or ""
            if uri and uri not in seen_uris:
                seen_uris.add(uri)
                if min_chars <= 0 or _passes_min_chars(item, min_chars):
                    good_count += 1
                    if good_count >= target_count:
                        done = True
                        break
        if done:
            break
        cursor = resp.cursor if hasattr(resp, "cursor") else None
        if not cursor or cursor == prev_cursor:
            break
        prev_cursor = cursor
    log.info("fetch_by_count: %d items in %.1fs (%d pages)", len(results), time.monotonic() - _t0, pages)
    return results


def fetch_by_time(fetch_fn, since_minutes: int, max_items: int | None = None,
                  min_chars: int = 0, ordered: bool = False) -> _Fetched:
    """Fetch every feed item inside a time window.

    `max_items` caps the result at the newest N *eligible* posts — unique and
    passing min_chars, counted exactly as fetch_by_count counts — so duplicate
    reposts and too-short posts never use up the caller's limit.
    _TIME_FETCH_CAP separately bounds the raw items held.

    An item older than the cutoff is skipped, not taken as the end of the
    window: ranked feeds interleave old posts among new ones. Only an `ordered`
    source — reverse-chronological in the very timestamp being filtered (the
    home timeline, list feeds) — ends at its first page that is entirely older
    than the cutoff. Anything else (feed generators, likes, which are ordered by
    like time but filtered by post time) may still hold newer posts further
    down, so its walk ends after _RANKED_OLD_PAGES consecutive all-old pages
    and the result says so.
    """
    _t0 = time.monotonic()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    results = _Fetched()
    seen_uris: set[str] = set()
    good_count = 0
    cursor = None
    prev_cursor = None
    empty_pages = 0
    old_streak = 0
    pages = 0
    while True:
        if pages >= _MAX_FEED_PAGES:
            results.notes.append(f"stopped after {_MAX_FEED_PAGES} pages; older posts in the window are missing")
            break
        try:
            resp = _retry_fetch(fetch_fn, cursor)
        except _UpstreamError as exc:
            results.notes.append(_walk_failed(exc, good_count, pages))
            break
        pages += 1
        feed = resp.feed if hasattr(resp, "feed") else []
        if not feed:
            empty_pages += 1
            if empty_pages >= 3:
                break
            cursor = resp.cursor if hasattr(resp, "cursor") else None
            if not cursor or cursor == prev_cursor:
                break
            prev_cursor = cursor
            continue
        empty_pages = 0
        done = False
        page_has_recent = False
        for item in feed:
            ts = _get_feed_item_timestamp(item)
            if ts is None or ts < cutoff:
                continue  # unparseable, or interleaved old post: skip, don't stop
            page_has_recent = True
            results.append(item)
            post = item.post if hasattr(item, "post") else item
            uri = getattr(post, "uri", "") or ""
            if uri and uri not in seen_uris:
                seen_uris.add(uri)
                if _passes_min_chars(item, min_chars):
                    good_count += 1
            if max_items is not None and good_count >= max_items:
                done = True
                break
            if len(results) >= _TIME_FETCH_CAP:
                results.notes.append(f"fetch capped at {_TIME_FETCH_CAP} items; older posts in the window are missing")
                done = True
                break
        if done:
            break
        if page_has_recent:
            old_streak = 0
        else:
            old_streak += 1
            if ordered:
                break  # chronological source: everything further down is older
            if old_streak >= _RANKED_OLD_PAGES:
                results.notes.append(
                    f"source is not in time order: stopped after {_RANKED_OLD_PAGES} pages "
                    "older than the window; recent posts further down may be missing"
                )
                break
        cursor = resp.cursor if hasattr(resp, "cursor") else None
        if not cursor or cursor == prev_cursor:
            break
        prev_cursor = cursor
    log.info("fetch_by_time: %d items in %.1fs (%d pages)", len(results), time.monotonic() - _t0, pages)
    return results


# --- Deduplication (matching reference script) ---


def _deduplicate_posts(items: list) -> list[tuple]:
    """Deduplicate feed items, merging reposts of the same post.

    Returns list of (canonical_item, reposters_list) tuples.
    """
    seen: dict[str, dict] = {}
    order: list[str] = []

    for item in items:
        post = item.post if hasattr(item, "post") else item
        uri = post.uri or ""

        reason = getattr(item, "reason", None)
        reason_type = getattr(reason, "py_type", "") or "" if reason else ""
        is_rp = "reasonRepost" in reason_type

        if not uri:
            continue

        if uri not in seen:
            seen[uri] = {"canonical": item, "reposters": [], "has_original": not is_rp}
            order.append(uri)
        else:
            entry = seen[uri]
            if not is_rp and not entry["has_original"]:
                entry["canonical"] = item
                entry["has_original"] = True

        if is_rp and reason:
            by = getattr(reason, "by", None)
            handle = getattr(by, "handle", None) if by else None
            if handle:
                entry = seen[uri]
                existing_handles = {r.get("handle") for r in entry["reposters"]}
                if handle not in existing_handles:
                    display_name = getattr(by, "display_name", None) or handle
                    entry["reposters"].append({
                        "handle": handle,
                        "display_name": display_name,
                    })

    result = []
    for uri in order:
        e = seen[uri]
        result.append((e["canonical"], e["reposters"]))
    return result


# --- Compact feed renderer (used by feed tools, search, likes, bookmarks) ---

_VIRAL_DROP_THRESHOLD = 10  # drop metrics field if total engagement < this
# Output budget for compact rows (~18-20k tokens of English), under Claude
# Code's default 25k-token tool-result cap with room for the header and quote
# table. Measured in UTF-8 bytes rather than characters: CJK text and emoji
# cost about a token per character, and bytes track that far better.
_RAW_OUTPUT_CHAR_BUDGET = int(os.getenv("MCP_RAW_OUTPUT_CHARS", "72000"))
# Digits charged per post ID while sizing rows, before IDs are allocated.
_ID_WIDTH_ESTIMATE = 6


def _out_size(text: str) -> int:
    return len(text.encode("utf-8"))


def _notes_line(notes) -> str:
    return ("⚠ " + "; ".join(notes) + "\n") if notes else ""


def _render_quote_entry(q_handle: str, q_time: str, q_text: str, strategy: str, parent_text: str) -> str:
    """Render the body portion of a quote table entry according to the chosen strategy."""
    if strategy == "none" or strategy == "author_only":
        return ""
    q_text = _sanitize_for_row(q_text)
    if strategy == "truncated_120":
        return _truncate(q_text, 120) if q_text else ""
    if strategy == "full_when_parent_text_short":
        return q_text if (parent_text and len(parent_text) < 80) else _truncate(q_text, 120)
    # "full"
    return q_text


def _render_compact_feed(
    items: list,
    since_minutes: int | None,
    quote_strategy: str,
    min_chars: int,
    max_posts: int | None = None,
    extra_cols: dict[str, str] | None = None,
    overflow_hint: str = 'narrow since_minutes/limit, or use mode="index" for the whole window',
) -> str:
    """
    Deduplicate, filter, scope, then render in the compact pipe-delimited schema.

    Every eligible post is rendered — max_posts caps the set to the newest N in
    feed order, and that is the only thinning that happens. To read a window too
    large to render row-by-row, use get_posts(mode="index") instead of asking
    for a sample of it.

    The one exception is the output size cap (_RAW_OUTPUT_CHAR_BUDGET): clients
    truncate or reject oversized tool results (Claude Code at 25k tokens by
    default), so rows past the cap are cut — newest kept, in feed order — and a
    footer says how many and how to narrow (overflow_hint). A visible cut, not a
    silent one and not a sample. Rows are sized before IDs are allocated, so
    only kept rows (and the in-batch parents they reply to) take an ID.

    Notes attached to `items` by the fetch (_Fetched.notes: partial walks,
    caps) are shown at the top so an incomplete window never reads as whole.

    extra_cols maps post AT URI -> a short string inserted as an extra column
    right after the time column (bookmarks use it for the bookmark date).
    """
    notes = list(getattr(items, "notes", None) or [])
    raw_count = len(items)

    deduped = _deduplicate_posts(items)
    dedup_count = len(deduped)

    if min_chars > 0:
        deduped = [(item, rp) for item, rp in deduped if _passes_min_chars(item, min_chars)]
        filtered = dedup_count - len(deduped)
    else:
        filtered = 0

    # Posts that survived dedup + min_chars and will go into the max_posts cap.
    eligible_count = len(deduped)

    if not deduped:
        return _notes_line(notes) + "No posts found."

    # Scope cap: newest max_posts (items arrive newest-first in feed order)
    if max_posts is not None and len(deduped) > max_posts:
        deduped = deduped[:max_posts]

    # --- Per-post pre-computation ---

    class _PostCandidate:
        __slots__ = (
            "item", "reposters", "post_obj", "post_text", "quoted",
            "quoted_uri", "ext_url", "author_key",
            "embed_type", "image_count", "is_reply", "embed_suffix", "ts",
        )

    candidates: list[_PostCandidate] = []
    for item, reposters in deduped:
        c = _PostCandidate()
        c.item = item
        c.reposters = reposters
        c.post_obj = item.post if hasattr(item, "post") else item
        record = getattr(c.post_obj, "record", None)
        c.post_text = _sanitize_for_row(_get_text(record)) if record else ""
        c.quoted = _extract_quoted_post(c.post_obj)  # (handle, display, ts_dt, text) or None
        c.quoted_uri = None
        if c.quoted:
            embed = getattr(c.post_obj, "embed", None)
            rec = getattr(embed, "record", None) if embed else None
            if rec:
                inner = getattr(rec, "record", rec)
                c.quoted_uri = getattr(inner, "uri", None)
        c.ext_url = _get_external_url(c.post_obj)
        author_obj = getattr(c.post_obj, "author", None)
        c.author_key = (getattr(author_obj, "did", None) or getattr(author_obj, "handle", "?")) if author_obj else "?"
        c.embed_type, c.image_count = _get_embed_type(c.post_obj)
        c.is_reply = bool(record and getattr(record, "reply", None))
        c.ts = _get_feed_item_timestamp(item)

        embed_suffix = ""
        if c.embed_type == "images":
            count_tag = f"img×{c.image_count}" if c.image_count > 1 else "img"
            # Posts with (almost) no text are unclassifiable from the row alone —
            # surface the alt text when the author provided one.
            alts = _get_image_alts(c.post_obj) if len(c.post_text) < 30 else []
            if alts:
                alt_str = _truncate(_sanitize_for_row("; ".join(alts)), 100)
                embed_suffix = f" [{count_tag}: {alt_str}]"
            else:
                embed_suffix = f" [{count_tag}]"
        elif c.embed_type == "video":
            embed_suffix = " [video]"
        elif c.embed_type == "url":
            embed_suffix = " [url]"
        c.embed_suffix = embed_suffix
        candidates.append(c)

    selected = candidates  # already in feed order

    if not selected:
        return _notes_line(notes) + "No posts found."

    # --- Notable author threshold (top 10% by like_count) ---

    like_counts = sorted(
        (c.post_obj.like_count or 0) for c in selected
    )
    # int(n * 0.9) gives the index of the 90th-percentile value; clamp to last index.
    p90_idx = min(int(len(like_counts) * 0.9), len(like_counts) - 1) if like_counts else 0
    p90_likes = like_counts[p90_idx] if like_counts else 0
    notable_threshold = max(p90_likes, 1)  # avoid flagging everything when all have 0 likes

    # --- Quote table (single pass; the first quoting post's text is kept for
    # the full_when_parent_text_short strategy, so no rescan later) ---

    quote_line_by_label: dict[str, str] = {}
    quote_uri_to_label: dict[str, str] = {}
    for c in selected:
        if not c.quoted or not c.quoted_uri or c.quoted_uri in quote_uri_to_label:
            continue
        label = f"Q{len(quote_uri_to_label) + 1}"
        quote_uri_to_label[c.quoted_uri] = label
        q_handle, _, q_ts_dt, q_text = c.quoted
        body = _render_quote_entry(q_handle, "", q_text, quote_strategy, c.post_text)
        entry_parts = [f"  {label}", q_handle]
        if q_ts_dt:
            # Quoted posts often predate the window — always include the day.
            entry_parts.append(_fmt_dt_compact(q_ts_dt, None))
        if body:
            entry_parts.append(body)
        quote_line_by_label[label] = " | ".join(entry_parts)

    # --- Row specs (everything but the IDs) ---

    uri_in_batch: set[str] = {c.post_obj.uri for c in selected}
    specs: list[dict] = []
    for c in selected:
        author = getattr(c.post_obj, "author", None)
        handle = author.handle if author else "?"
        like_count = c.post_obj.like_count or 0
        if like_count >= notable_threshold and (author and author.display_name and author.display_name != handle):
            author_field = f"{handle} ({_sanitize_for_row(author.display_name)})"
        else:
            author_field = handle

        total_eng = like_count + (c.post_obj.repost_count or 0) + (c.post_obj.reply_count or 0)
        metrics_str = (
            f"{like_count},{c.post_obj.repost_count or 0},{c.post_obj.reply_count or 0}"
            if total_eng >= _VIRAL_DROP_THRESHOLD else ""
        )

        # refs: reply and/or quote
        parent_in_batch = None
        reply_handle_ref = None
        record = getattr(c.post_obj, "record", None)
        if record and hasattr(record, "reply") and record.reply:
            parent_uri = ""
            if hasattr(record.reply, "parent") and record.reply.parent:
                parent_uri = getattr(record.reply.parent, "uri", "")
            if parent_uri in uri_in_batch:
                parent_in_batch = parent_uri
            else:
                # Try to get the parent handle from feed_view.reply.
                # fv_reply.parent may be a BlockedPost / NotFoundPost whose author
                # lacks .handle (BlockedAuthor only carries did + viewer).
                fv_reply = getattr(c.item, "reply", None)
                if fv_reply and hasattr(fv_reply, "parent"):
                    pa = getattr(fv_reply.parent, "author", None)
                    reply_handle_ref = getattr(pa, "handle", None) if pa else None

        specs.append({
            "uri": c.post_obj.uri,
            "head": [author_field, _fmt_ts_compact(c.item, since_minutes)]
                    + ([extra_cols[c.post_obj.uri]] if extra_cols and extra_cols.get(c.post_obj.uri) else [])
                    + ([metrics_str] if metrics_str else []),
            "parent": parent_in_batch,
            "reply_handle": reply_handle_ref,
            "label": quote_uri_to_label.get(c.quoted_uri) if c.quoted and c.quoted_uri else None,
            "text": c.post_text + c.embed_suffix,
        })

    def render_row(spec, id_of) -> str:
        refs = []
        if spec["parent"]:
            refs.append(f"Re:{id_of(spec['parent'])}")
        elif spec["reply_handle"]:
            refs.append(f"Re:@{spec['reply_handle']}")
        if spec["label"]:
            refs.append(f"Q:{spec['label']}")
        parts = [str(id_of(spec["uri"])), *spec["head"]]
        if refs:
            parts.append(" ".join(refs))
        parts.append(spec["text"])
        return "  " + " | ".join(parts)

    # --- Output size cap ---
    # Keep rows newest-first until the budget runs out. A quote entry is
    # charged to the first kept row that references it; labels are assigned in
    # first-reference order, so the kept quotes are always Q1..Qk.

    placeholder = "9" * _ID_WIDTH_ESTIMATE
    kept: list[dict] = []
    quote_lines: list[str] = []
    kept_labels: set[str] = set()
    used = 0
    for spec in specs:
        label = spec["label"]
        new_quote = label is not None and label not in kept_labels
        cost = _out_size(render_row(spec, lambda _uri: placeholder)) + 1
        if new_quote:
            cost += _out_size(quote_line_by_label[label]) + 1
        if kept and used + cost > _RAW_OUTPUT_CHAR_BUDGET:
            break
        used += cost
        kept.append(spec)
        if new_quote:
            kept_labels.add(label)
            quote_lines.append(quote_line_by_label[label])
    omitted = len(specs) - len(kept)

    # --- Store kept posts (and the parents they cite) in SQLite, then render ---

    id_uris = list(dict.fromkeys(
        [s["uri"] for s in kept] + [s["parent"] for s in kept if s["parent"]]
    ))
    uri_to_id = dict(zip(id_uris, _db_store_posts(id_uris)))
    kept_rows = [render_row(spec, uri_to_id.__getitem__) for spec in kept]

    # --- Build final output ---

    shown = len(kept_rows)
    # Funnel: raw → eligible (after dedup + min_chars) → shown (after the cap)
    eligible_label = f"{eligible_count} eligible"
    if filtered:
        eligible_label += f" ({filtered} too short)"
    funnel_parts = [f"{raw_count} raw", eligible_label]
    if shown < eligible_count:
        funnel_parts.append(f"{shown} shown")
    funnel = " → ".join(funnel_parts)

    window_note = f" | window: {since_minutes}min" if since_minutes is not None else ""
    # The legend replaces the row-format section the server instructions used
    # to carry: it costs tokens only when a feed tool actually runs.
    bm_col = "bm:date | " if extra_cols else ""
    header = (
        f"{shown} posts | {funnel}{window_note}\n"
        + _notes_line(notes)
        + f"cols: id | author | time | {bm_col}likes,reposts,replies (if ≥10) | "
        f"refs (Re:<id|@handle> reply, Q:Qn quote) | text ('¦' = '|')\n"
        f"IDs valid 1h · get_post_by_id(N) for full detail · {URL_TEMPLATE_NOTE}\n"
    )
    mutuals_line = _mutuals_line(c.post_obj for c in selected)
    if mutuals_line:
        header += mutuals_line + "\n"

    sections: list[str] = [header]
    if quote_lines:
        sections.append("quotes:\n" + "\n".join(quote_lines))
    sections.append("posts:\n" + "\n".join(kept_rows))
    if omitted:
        sections.append(
            f"… {omitted} more post{'s' if omitted != 1 else ''} not shown "
            f"(output size cap); {overflow_hint}."
        )
    return "\n".join(sections)


# --- Timeline index (mode="index") ---
#
# The vendored `comp` package compresses a window of posts into keyqueries:
# term conjunctions that both NAME a discussion and ARE the search query that
# retrieves it. A 3,000-post window renders in ~520 tokens instead of ~143,000,
# and nothing is discarded — the detail is recovered on demand by re-issuing an
# entry through search_posts. See CLAUDE.md for the provenance of the
# background artifacts in data/bg/.

_comp_bg = None
_comp_priors = None
_comp_lock = threading.Lock()


def _comp_artifacts():
    """The 41 MB background index and subject priors, loaded once on first use.

    Deliberately lazy: a server that never builds an index should not pay the
    memory. Guarded by a lock because tools run in worker threads.
    """
    global _comp_bg, _comp_priors
    if _comp_bg is None or _comp_priors is None:
        with _comp_lock:
            if _comp_bg is None or _comp_priors is None:
                from comp.bg import Background
                from comp.vocab import Priors
                t0 = time.time()
                _comp_bg, _comp_priors = Background(), Priors()
                log.info("index artifacts loaded in %.1fs", time.time() - t0)
    return _comp_bg, _comp_priors


def _record_tags(record) -> str:
    """Hashtags on a post: the `tags` field plus any #tag richtext facets."""
    tags = list(getattr(record, "tags", None) or [])
    for facet in getattr(record, "facets", None) or []:
        for feature in getattr(facet, "features", None) or []:
            tag = getattr(feature, "tag", None)
            if tag:
                tags.append(tag)
    return " ".join(dict.fromkeys(t.lower() for t in tags if t))


def _postviews_to_frame(deduped):
    """Build the frame comp/ expects from deduplicated feed items.

    Column contract mirrors comp/data.py: the SEARCHABLE fields (text,
    ext_title, ext_desc, alt_text, tags) feed the analyzer, reply_root and
    quote_uri build the thread and discussion units, and ext_uri supplies the
    shared-article merge edges.
    """
    import pandas as pd

    rows = []
    for item, _reposters in deduped:
        post = item.post if hasattr(item, "post") else item
        record = getattr(post, "record", None)
        author = getattr(post, "author", None)
        uri = getattr(post, "uri", "") or ""

        # The feed item's timestamp, NOT record.created_at: created_at is
        # client-set and occasionally a wild lie (one live fetch carried a post
        # backdated to 2023, which stretched the reported window to 1045 days
        # and would have sent every expansion search to the wrong bounds).
        # _get_feed_item_timestamp is server-assigned indexed_at, resolves
        # reposts to the repost time, and is the exact field fetch_by_time
        # filtered on — so the window in the header is the window fetched.
        ts = _get_feed_item_timestamp(item)

        reply = getattr(record, "reply", None) if record else None
        reply_root = getattr(getattr(reply, "root", None), "uri", None) if reply else None
        reply_parent = getattr(getattr(reply, "parent", None), "uri", None) if reply else None

        quote_uri = None
        embed = getattr(post, "embed", None)
        rec = getattr(embed, "record", None) if embed else None
        if rec:
            quote_uri = getattr(getattr(rec, "record", rec), "uri", None)

        ext_uri, ext_title, ext_desc = _get_external_meta(post)
        langs = getattr(record, "langs", None) if record else None

        rows.append({
            "uri": uri,
            "cid": getattr(post, "cid", "") or "",
            "rkey": uri.rsplit("/", 1)[-1],
            "did": getattr(author, "did", "") or "",
            "handle": getattr(author, "handle", "") or "?",
            "created_at": ts.isoformat() if ts else
                          (getattr(record, "created_at", None) if record else None),
            "text": _get_text(record) if record else "",
            "langs": " ".join(langs) if langs else None,
            "reply_root": reply_root,
            "reply_parent": reply_parent,
            "quote_uri": quote_uri,
            "ext_uri": ext_uri or None,
            "ext_title": ext_title or None,
            "ext_desc": ext_desc or None,
            "alt_text": " ".join(_get_image_alts(post)) or None,
            "tags": _record_tags(record) or None,
            "n_likes": post.like_count or 0,
            "n_reposts": post.repost_count or 0,
            "n_replies": post.reply_count or 0,
        })
    return pd.DataFrame(rows)


def _index_top_domains(df, k=8):
    """Most-linked hosts in the window (canonicalised, tracking params dropped)."""
    from comp.units import canon_url
    counter = Counter()
    for uri in df.ext_uri:
        if uri:
            canon = canon_url(uri)
            if canon:
                counter[canon.split("/")[0]] += 1
    return [d for d, _ in counter.most_common(k)]


def _index_top_authors(df, k=6):
    return [h.removesuffix(".bsky.social")
            for h, _ in Counter(df.handle).most_common(k)]


# Index builds are memory-heavy (dense term x unit matrices on top of the
# ~200 MB of background artifacts), so concurrent requests queue rather than
# multiply the peak. One at a time unless configured otherwise.
_INDEX_BUILD_SLOTS = threading.BoundedSemaphore(max(1, int(os.getenv("MCP_INDEX_CONCURRENCY", "1"))))


def _render_index(items, source: str, budget: int, max_posts: int | None = None) -> str:
    """Compress a fetched window into keyqueries. See the section note above."""
    from comp import data, encode, pipeline

    notes = list(getattr(items, "notes", None) or [])
    deduped = _deduplicate_posts(items)
    if max_posts is not None and len(deduped) > max_posts:
        deduped = deduped[:max_posts]
    if not deduped:
        return _notes_line(notes) + "No posts found."

    df = _postviews_to_frame(deduped)
    bg, priors = _comp_artifacts()

    with _INDEX_BUILD_SLOTS:
        _raise_if_cancelled()
        t0 = time.time()
        ctx = pipeline.prepare(None, frame=df, bg=bg, priors=priors)
        # prefix_budget, not budget: the ranking is built without regard to
        # cost and cut where it stops fitting, so a larger budget only appends
        # entries (see select.choose).
        result = pipeline.run(None, ctx=ctx, choose_kw={"prefix_budget": budget})
        chosen = result["chosen"]
        elapsed = time.time() - t0

    if source.lower() in ("timeline", "home", "following"):
        scope, source_note = encode.SCOPE_FOLLOWING, None
    else:
        # Nothing in search reproduces "this feed", so say so rather than
        # emitting a `following=1` that would quietly search the wrong set.
        scope = encode.SCOPE_OTHER
        source_note = (f"Index is of '{source}'; search is global, so expect "
                       f"some hits from outside it.")
    if notes:
        source_note = " ".join(filter(None, [source_note, "⚠ " + "; ".join(notes) + "."]))

    text = encode.encode(
        chosen, ctx["units"], data.window(ctx["df"]),
        n_posts=len(ctx["df"]), n_authors=ctx["df"].handle.nunique(),
        sep="; ",
        residual=encode.residual_line(chosen, ctx["units"]),
        domains=_index_top_domains(ctx["df"]),
        top_authors=_index_top_authors(ctx["df"]),
        mutuals=_mutuals_present_handles(zip(df.did, df.handle)),
        scope=scope, source_note=source_note,
    )
    log.info("index: %d posts -> %d entries in %.1fs",
             len(ctx["df"]), len(chosen), elapsed)
    return text


# --- Link card / embed helpers ---
#
# Outbound fetches of caller-influenced URLs (link cards, image CDN URLs) go
# through _public_http, whose transport checks the address it actually
# connects to. Checking a hostname up front and then letting the HTTP client
# resolve it again is not enough: another A/AAAA record, an IPv6 answer or a
# DNS rebind can point the real connection somewhere private.

_USER_AGENT = "Mozilla/5.0 (compatible; bsky-mcp-bot/1.0)"
_EMBED_RE = re.compile(r"^\[embed:\s*(https?://\S+)\]\s*$", re.MULTILINE)
_POST_IMAGE_MAX_BYTES = 5_000_000
_POST_IMAGES_TOTAL_MAX_BYTES = 40_000_000
# OG tags live in <head>; a page is read up to this much and the rest ignored.
_LINK_CARD_MAX_BYTES = 1_000_000
_FETCH_DEADLINE = 20.0  # seconds per fetch, all redirect hops and body included


def _is_public_ip(raw: str) -> bool:
    """True only for globally routable unicast addresses."""
    try:
        addr = ipaddress.ip_address(raw.split("%", 1)[0])
    except ValueError:
        return False
    mapped = getattr(addr, "ipv4_mapped", None)
    if mapped is not None:
        addr = mapped
    return addr.is_global and not (
        addr.is_private or addr.is_loopback or addr.is_link_local
        or addr.is_multicast or addr.is_reserved or addr.is_unspecified
    )


class _PublicOnlyBackend(httpcore.SyncBackend):
    """Resolves the host itself, refuses if ANY answer is non-public, then
    connects to a vetted address. TLS still verifies against the hostname
    (httpcore passes the origin host as server_hostname to start_tls)."""

    def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise httpcore.ConnectError(f"cannot resolve {host}: {exc}") from exc
        addrs = list(dict.fromkeys(info[4][0] for info in infos))
        blocked = [a for a in addrs if not _is_public_ip(a)]
        if not addrs or blocked:
            raise httpcore.ConnectError(f"refusing non-public address for {host}: {blocked or 'none'}")
        last_exc: Exception | None = None
        for addr in addrs:
            try:
                return super().connect_tcp(addr, port, timeout=timeout, local_address=local_address,
                                           socket_options=socket_options)
            except httpcore.ConnectError as exc:
                last_exc = exc
        raise last_exc


class _PublicOnlyTransport(httpx.HTTPTransport):
    def __init__(self):
        # trust_env=False: an environment proxy would resolve the target itself
        # and bypass the address check.
        super().__init__(trust_env=False)
        # httpx exposes no network_backend option; swap the pool it built for
        # an equivalent one using the vetting backend.
        assert isinstance(self._pool, httpcore.ConnectionPool)
        self._pool = httpcore.ConnectionPool(
            ssl_context=httpx.create_ssl_context(trust_env=False),
            network_backend=_PublicOnlyBackend(),
        )


_public_http = httpx.Client(
    transport=_PublicOnlyTransport(),
    timeout=httpx.Timeout(10.0, connect=5.0),
    follow_redirects=False,
    trust_env=False,
    headers={"User-Agent": _USER_AGENT, "Accept-Encoding": "identity"},
)


def _is_safe_url(url: str) -> bool:
    """Cheap pre-check: http(s) scheme, a host, and not a literal private IP.

    The authoritative address check happens at connect time (_PublicOnlyBackend).
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False
    host = parsed.hostname
    try:
        ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return True  # a name; vetted when the connection is made
    return _is_public_ip(host)


def _fetch_public(url: str, max_bytes: int, *, truncate: bool = False,
                  check_headers=None, max_redirects: int = 5) -> tuple[httpx.Response, bytes]:
    """GET a public URL with redirect, size and time limits enforced while reading.

    Redirects are followed by hand so each hop is re-checked. The body is
    streamed: a declared Content-Length over the limit is refused before any
    body is read, and the running byte count is checked per chunk, so an
    oversized or endless response never sits in memory. `truncate=True` stops
    at max_bytes instead of failing (HTML whose useful part comes first).
    `check_headers(response)` may raise to reject a response before its body.
    """
    deadline = time.monotonic() + _FETCH_DEADLINE
    for _ in range(max_redirects + 1):
        if not _is_safe_url(url):
            raise ValueError(f"non-public URL: {url}")
        with _public_http.stream("GET", url) as resp:
            location = resp.headers.get("location")
            if resp.status_code in (301, 302, 303, 307, 308) and location:
                url = str(httpx.URL(url).join(location))
                continue
            resp.raise_for_status()
            if check_headers:
                check_headers(resp)
            try:
                declared = int(resp.headers.get("content-length", "0"))
            except ValueError:
                declared = 0
            if declared > max_bytes and not truncate:
                raise ValueError(f"response too large ({declared} bytes; max {max_bytes})")
            buf = bytearray()
            for chunk in resp.iter_bytes():
                buf += chunk
                if len(buf) > max_bytes:
                    if truncate:
                        del buf[max_bytes:]
                        break
                    raise ValueError(f"response too large (over {max_bytes} bytes)")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"fetch exceeded {_FETCH_DEADLINE:.0f}s: {url}")
            return resp, bytes(buf)
    raise ValueError(f"too many redirects: {url}")


def _check_image_headers(resp: httpx.Response) -> None:
    mime_type = resp.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if not mime_type.startswith("image/") or mime_type == "image/svg+xml":
        raise ValueError(f"unexpected image content type: {mime_type or 'missing'}")


def _download_post_image(url: str) -> tuple[bytes, str]:
    """Download one API-supplied image URL and validate its type and size."""
    resp, data = _fetch_public(url, _POST_IMAGE_MAX_BYTES, check_headers=_check_image_headers)
    mime_type = resp.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    return data, mime_type


def _fetch_link_card(url: str) -> dict:
    """Fetch OG title and description for a URL to build a link card embed."""
    try:
        resp, body = _fetch_public(url, _LINK_CARD_MAX_BYTES, truncate=True)
        html = body.decode(resp.charset_encoding or "utf-8", errors="replace")

        def _meta(prop: str) -> str:
            for pattern in [
                rf'<meta[^>]+property=["\']og:{prop}["\'][^>]+content=["\']([^"\']*)["\']',
                rf'<meta[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']og:{prop}["\']',
            ]:
                m = re.search(pattern, html, re.I)
                if m:
                    return m.group(1).strip()
            return ""

        title = _meta("title")
        description = _meta("description")
        if not title:
            m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip()
        return {"uri": url, "title": title, "description": description}
    except Exception as e:
        log.warning("Link card fetch failed for %s: %s", url, e)
        return {"uri": url, "title": "", "description": ""}


def _build_external_embed(url: str) -> models.AppBskyEmbedExternal.Main:
    card = _fetch_link_card(url)
    return models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            uri=card["uri"],
            title=card["title"],
            description=card["description"],
        )
    )


# --- Rich text facets ---
# AT Protocol clients never auto-linkify plain text: without explicit facet
# records (byte ranges into the UTF-8 text), URLs render dead, mentions don't
# notify, and hashtags aren't tappable. _test_facets.py exercises this exact code.

_FACET_URL_RE = re.compile(
    r"https?://[^\s<>\"']+|www\.[^\s<>\"']+|\b[a-z0-9][-a-z0-9.]*\.[a-z]{2,}/[^\s<>\"']*",
    re.IGNORECASE,
)
# Handles always contain at least one dot (user.bsky.social); requiring it
# keeps emails and bare "@someone" out.
_FACET_MENTION_RE = re.compile(r"(?<![\w@])@([a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,})")
_FACET_TAG_RE = re.compile(r"(?<![\w#])#([^\W\d_][\w]*)", re.UNICODE)
_URL_TRAILING_PUNCT = ".,;:!?'\")]}»…"


def _trim_url_match(url: str) -> str:
    """Strip sentence punctuation that the URL regex greedily absorbed."""
    while url and url[-1] in _URL_TRAILING_PUNCT:
        if url[-1] == ")" and url.count("(") >= url.count(")"):
            break  # balanced paren, e.g. en.wikipedia.org/wiki/Foo_(bar)
        url = url[:-1]
    return url


def _resolve_handle_did(handle: str) -> str:
    return client.com.atproto.identity.resolve_handle({"handle": handle}).did


def _detect_facets(text: str, resolve_handle=None) -> list[models.AppBskyRichtextFacet.Main] | None:
    """Detect links, mentions, and hashtags; return facets with UTF-8 byte offsets.

    Mentions are resolved to DIDs via the API (`resolve_handle`, injectable for
    tests); handles that fail to resolve are left as plain text. URLs claim
    their character range first so a #fragment or @ inside a URL is never
    double-faceted.
    """
    resolve_handle = resolve_handle or _resolve_handle_did
    facets: list[models.AppBskyRichtextFacet.Main] = []
    claimed: list[tuple[int, int]] = []

    def overlaps(start: int, end: int) -> bool:
        return any(s < end and start < e for s, e in claimed)

    def byte_slice(start: int, end: int) -> models.AppBskyRichtextFacet.ByteSlice:
        return models.AppBskyRichtextFacet.ByteSlice(
            byte_start=len(text[:start].encode("utf-8")),
            byte_end=len(text[:end].encode("utf-8")),
        )

    for m in _FACET_URL_RE.finditer(text):
        url = _trim_url_match(m.group(0))
        if not url:
            continue
        start, end = m.start(), m.start() + len(url)
        uri = url if url.lower().startswith(("http://", "https://")) else f"https://{url}"
        facets.append(models.AppBskyRichtextFacet.Main(
            index=byte_slice(start, end),
            features=[models.AppBskyRichtextFacet.Link(uri=uri)],
        ))
        claimed.append((start, end))

    for m in _FACET_MENTION_RE.finditer(text):
        start, end = m.start(), m.end()
        if overlaps(start, end):
            continue
        handle = m.group(1)
        try:
            did = resolve_handle(handle)
        except Exception:
            log.info("Mention @%s did not resolve; leaving as plain text", handle)
            continue
        facets.append(models.AppBskyRichtextFacet.Main(
            index=byte_slice(start, end),
            features=[models.AppBskyRichtextFacet.Mention(did=did)],
        ))
        claimed.append((start, end))

    for m in _FACET_TAG_RE.finditer(text):
        start, end = m.start(), m.end()
        if overlaps(start, end):
            continue
        tag = m.group(1)
        # Lexicon caps: 640 bytes / 64 graphemes per tag (sans '#').
        if len(tag.encode("utf-8")) > 640 or grapheme.length(tag) > 64:
            continue
        facets.append(models.AppBskyRichtextFacet.Main(
            index=byte_slice(start, end),
            features=[models.AppBskyRichtextFacet.Tag(tag=tag)],
        ))
        claimed.append((start, end))

    return facets or None


def _validate_post_text(text: str, label: str = "Post") -> None:
    """Enforce both lexicon limits: 300 graphemes AND 3000 UTF-8 bytes."""
    length = grapheme.length(text)
    if length > 300:
        raise ValueError(f"{label} is {length} graphemes, exceeding the 300 grapheme limit. Please shorten it.")
    byte_len = len(text.encode("utf-8"))
    if byte_len > 3000:
        raise ValueError(f"{label} is {byte_len} UTF-8 bytes, exceeding the 3000 byte limit. Please shorten it.")


def _validate_message_text(text: str) -> None:
    """Enforce the DM lexicon limits: 1000 graphemes AND 10000 UTF-8 bytes."""
    if not text or not text.strip():
        raise ValueError("Message text is empty.")
    length = grapheme.length(text)
    if length > 1000:
        raise ValueError(f"Message is {length} graphemes, exceeding the 1000 grapheme limit. Please shorten it.")
    byte_len = len(text.encode("utf-8"))
    if byte_len > 10000:
        raise ValueError(f"Message is {byte_len} UTF-8 bytes, exceeding the 10000 byte limit. Please shorten it.")


# --- MCP Server ---

__version__ = "2.0.0"

# Server instructions sit in every model call (Claude Code puts them in the
# system prompt), even for clients that hide some tools, so only what is
# shared across tools lives here. What one tool does belongs in its docstring,
# what a parameter means in its description, and how to read an output in a
# header line of that output.
_SERVER_INSTRUCTIONS = f"""\
Bluesky access for @{BSKY_HANDLE} (read, post, lists, drafts, DMs).
Posts in tool output carry numeric IDs valid for 1 hour (get_post_by_id and
get_post_images take them); accounts are referenced by handle or DID; times
are America/Toronto. Feed-shaped output starts with a 'cols:' line describing
its rows.
Mutuals: the user's hand-picked roster of accounts that matter to them. Feed
output names those present on a 'mutuals here:' line; treat them as
significant, not as just more names in the feed.
Workflow: find topics with get_posts(mode="index"), expand one with
search_posts, read the conversation with get_thread. For what followed
accounts said about a story, use search_posts(following=True, query, time
bound) instead of pulling the timeline and filtering it.
Accounts: search_actors, then get_profile / get_author_posts /
get_known_followers (social context, not proof of trust).
DMs: a group chat is just a convo; get_conversations gives the convo_id the
other chat tools take.
"""


# --- Client argument shim ---
# Some MCP clients send `wait_for_previous` (a sequencing hint) on every call,
# and models sometimes reach for a sibling tool's parameter name (search's
# since_hours on a feed tool, the old singular author/lang). FastMCP 4 rejects
# undeclared arguments and has no exclude_args, so this middleware rewrites
# the arguments before validation: the schema stays clean while those calls
# keep working. Each alias maps to (canonical name, value transform).
def _as_list(value):
    return [value] if isinstance(value, str) else value


_SINCE_HOURS_ALIAS = {"since_hours": ("since_minutes", lambda h: max(1, round(h * 60)))}
_ARG_ALIASES: dict[str, dict] = {
    "search_posts": {
        "author": ("authors", _as_list),
        "lang": ("languages", _as_list),
        "since_minutes": ("since_hours", lambda m: m / 60),
    },
    "get_posts": _SINCE_HOURS_ALIAS,
    "get_my_likes": _SINCE_HOURS_ALIAS,
    "get_notifications": _SINCE_HOURS_ALIAS,
    "get_messages": _SINCE_HOURS_ALIAS,
}


class _SessionGuard(Middleware):
    """Sign in, refresh, or replace a dead session before every tool call.

    Covers a login that failed at startup (Bluesky down, rate limited: the
    server stays up and retries here, with backoff), and repairs a session the
    server has invalidated before SDK-only tools run into it — _ensure_session
    otherwise only runs ahead of raw XRPC and chat calls. A transient failure
    with a session in hand is logged and the call goes ahead; the tool's own
    requests then succeed or report the error themselves.
    """

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        try:
            await asyncio.to_thread(_ensure_session)
        except Exception as exc:
            if client.me is None:
                raise ToolError(f"Not signed in to Bluesky: {_brief_error(exc)}") from exc
            log.warning("Session check before %s failed (%s); trying the call anyway",
                        context.message.name, _brief_error(exc))
        return await call_next(context)


class _ClientArgShim(Middleware):
    async def on_call_tool(self, context: MiddlewareContext, call_next):
        args = context.message.arguments
        if args:
            args.pop("wait_for_previous", None)
            for alias, (canonical, transform) in _ARG_ALIASES.get(context.message.name, {}).items():
                if alias not in args:
                    continue
                value = args.pop(alias)
                if value is None:
                    continue
                if args.get(canonical) is not None:
                    raise ToolError(f"Pass either {canonical} or {alias}, not both.")
                args[canonical] = transform(value)
        return await call_next(context)


# --- Auth ---


class _HeaderTokenVerifier(TokenVerifier):
    """Accepts the one static token from MCP_AUTH_TOKEN (CLIs, bots, scripts)."""

    def __init__(self, token: str):
        super().__init__()
        self._token = token.encode()

    async def verify_token(self, token: str) -> AccessToken | None:
        if hmac.compare_digest(token.encode(), self._token):
            return AccessToken(token=token, client_id="header-token", scopes=[], subject="header-token")
        return None


class _AllowlistedGitHubProvider(GitHubProvider):
    """GitHub OAuth that only lets the listed GitHub accounts in.

    GitHubProvider alone admits any GitHub user who completes the flow. The
    check runs on every request (load_access_token backs verify_token), so a
    token minted for anyone else is useless even though the flow succeeded.
    """

    def __init__(self, *, allowed_ids: set[str], **kwargs):
        super().__init__(**kwargs)
        self._allowed_ids = allowed_ids

    async def load_access_token(self, token: str) -> AccessToken | None:
        result = await super().load_access_token(token)
        if result is None:
            return None
        github_id = str((result.claims or {}).get("sub") or result.subject or "")
        if github_id not in self._allowed_ids:
            log.warning("Rejected GitHub account %s (%s): not in MCP_ALLOWED_GITHUB_IDS",
                        (result.claims or {}).get("login"), github_id)
            return None
        return result


# Redirect URIs an OAuth client may register: ChatGPT, Claude apps, and the
# loopback callbacks of CLI clients. The consent screen and account allowlist
# are the main guards; this narrows phishing-style registrations further.
_OAUTH_REDIRECT_PATTERNS = [
    "https://chatgpt.com/connector/oauth/*",
    "https://chatgpt.com/connector_platform_oauth_redirect",
    "https://claude.ai/*",
    "https://claude.com/*",
    "http://localhost:*",
    "http://127.0.0.1:*",
]


def _build_auth():
    """Header token, GitHub OAuth, or both, depending on what .env configures."""
    header = _HeaderTokenVerifier(MCP_AUTH_TOKEN) if MCP_AUTH_TOKEN else None
    github = None
    if GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET:
        missing = [name for name, value in (
            ("GITHUB_CLIENT_ID", GITHUB_CLIENT_ID),
            ("GITHUB_CLIENT_SECRET", GITHUB_CLIENT_SECRET),
            ("MCP_PUBLIC_URL", MCP_PUBLIC_URL),
            ("MCP_ALLOWED_GITHUB_IDS", MCP_ALLOWED_GITHUB_IDS),
        ) if not value]
        if missing:
            raise SystemExit(f"GitHub OAuth is half-configured; set {', '.join(missing)} in .env")
        github = _AllowlistedGitHubProvider(
            allowed_ids=MCP_ALLOWED_GITHUB_IDS,
            client_id=GITHUB_CLIENT_ID,
            client_secret=GITHUB_CLIENT_SECRET,
            base_url=MCP_PUBLIC_URL,
            # No scopes: the server only needs to know who signed in, and a
            # scopeless GitHub token can read nothing beyond public profile
            # data. It also keeps MultiAuth's shared scope check satisfiable
            # by the header token.
            required_scopes=[],
            cache_ttl_seconds=300,  # verification calls the GitHub API twice
            allowed_client_redirect_uris=_OAUTH_REDIRECT_PATTERNS,
            jwt_signing_key=os.getenv("MCP_JWT_SIGNING_KEY") or None,
        )
    if github and header:
        return MultiAuth(server=github, verifiers={"header-token": header})
    return github or header


mcp = FastMCP(
    "bluesky",
    instructions=_SERVER_INSTRUCTIONS,
    version=__version__,
    middleware=[_SessionGuard(), _ClientArgShim()],
    auth=_build_auth(),
)


_HEARTBEAT_INTERVAL = float(os.getenv("MCP_HEARTBEAT_INTERVAL", "2.0"))


@asynccontextmanager
async def _heartbeat(ctx: Context | None, interval: float = _HEARTBEAT_INTERVAL):
    """Emit notifications/progress every `interval` seconds during long tool work.

    Per the MCP 2025-06-18 lifecycle spec, implementations MAY reset the
    request timeout when receiving a progress notification ("this implies
    that work is actually happening"). Claude.AI / ChatGPT enforce a ~60s
    tool-call deadline; without these beats a wide-window fetch or a big
    mode="index" build outlives the deadline even though the server completes
    it fine.

    We fire an immediate beat on entry so the client gets evidence of work
    within the first event-loop tick (the atproto SDK and the index's 41 MB
    background artifacts can burn seconds before the first natural interval).
    """
    if ctx is None:
        yield
        return

    counter = 0

    async def _emit():
        nonlocal counter
        counter += 1
        try:
            await ctx.report_progress(counter, None, "working...")
            log.debug("heartbeat #%d", counter)
        except Exception:
            pass

    async def _beat():
        while True:
            await asyncio.sleep(interval)
            await _emit()

    # Fire one beat immediately so the deadline clock resets before the
    # first sleep elapses. fastmcp's report_progress no-ops if the client
    # didn't send a progressToken, so this is safe regardless.
    await _emit()
    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        if counter:
            log.info("heartbeat: %d beats sent", counter)


# --- Tool registration ---
# Every tool passes output_schema=None. Left to infer, FastMCP turns `-> str`
# into a {"result": string} output schema and sends each result twice: as
# text and as structuredContent. Claude Code hands the model the JSON copy,
# so the row tables arrived as one escaped line costing extra tokens. These
# tools return text meant to be read, so text is all they send.

# --- Tool annotations (MCP 2025-06-18 §Tools/Annotations) ---
# Annotations are hints to the client about how a tool behaves so it can
# render confirmation UI, group calls, and reason about retries. They are
# advisory — untrusted servers may lie, but for our own deployment they
# meaningfully improve client UX.
_ANN_READ_ONLY = {
    "readOnlyHint": True,
    "openWorldHint": True,  # Bluesky AppView is an external service
}
_ANN_WRITE_NEW = {
    # Creating a post is observable in the outside world but does not
    # delete or overwrite anything; clients can warn but don't need an
    # "are you sure?" prompt.
    "readOnlyHint": False,
    "destructiveHint": False,
    "idempotentHint": False,
    "openWorldHint": True,
}
_ANN_WRITE_IDEMPOTENT = {
    "readOnlyHint": False,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": True,
}
_ANN_DESTRUCTIVE = {
    # Bookmark / draft deletion: irrecoverable from the user's POV.
    "readOnlyHint": False,
    "destructiveHint": True,
    "idempotentHint": True,
    "openWorldHint": True,
}


@mcp.tool(
    output_schema=None,
    annotations={"title": "List feed and list sources", **_ANN_READ_ONLY},
)
async def list_sources(ctx: Context = None) -> str:
    """List the saved feeds and curation lists usable as get_posts(source=...)."""
    async with _heartbeat(ctx):
        return await _run_tool(_list_sources_impl)


def _list_sources_impl() -> str:
    feed_lines = []
    for key, gen in refresh_feed_cache():
        name = gen.display_name or gen.uri
        desc = getattr(gen, "description", "") or ""
        desc_line = f"  {_truncate(desc, 100)}" if desc else ""
        # Show the disambiguated cache key for duplicate names — it's the
        # name get_posts actually resolves; the bare name only reaches the
        # first duplicate.
        label = name if key == name.lower() else key
        feed_lines.append(f"- {label} ({gen.uri}){desc_line}")

    list_lines = []
    for key, lst in refresh_list_cache():
        count = getattr(lst, "list_item_count", "?")
        label = lst.name if key == lst.name.lower() else key
        list_lines.append(f"- {label} ({lst.purpose}) [{count} members] ({lst.uri})")

    return (
        "Saved feeds:\n" + ("\n".join(feed_lines) or "  (none)")
        + "\n\nMy lists:\n" + ("\n".join(list_lines) or "  (none)")
    )


# --- List management ---


def _validate_list_text(name: str, description: str | None) -> tuple[str, str | None]:
    name = name.strip()
    if not name:
        raise ValueError("List name cannot be empty.")
    if grapheme.length(name) > 64 or len(name.encode("utf-8")) > 64:
        raise ValueError("List name must be at most 64 graphemes and 64 UTF-8 bytes.")
    if description is not None:
        description = description.strip()
        if grapheme.length(description) > 300 or len(description.encode("utf-8")) > 3000:
            raise ValueError(
                "List description must be at most 300 graphemes and 3000 UTF-8 bytes."
            )
    return name, description


def _raw_profile_flags(profile: dict) -> str:
    viewer = profile.get("viewer") or {}
    flags = []
    if viewer.get("following"):
        flags.append("following")
    if viewer.get("followedBy"):
        flags.append("follows-you")
    if viewer.get("muted"):
        flags.append("muted")
    if viewer.get("blocking"):
        flags.append("blocking")
    if viewer.get("blockedBy"):
        flags.append("blocked-you")
    if (profile.get("verification") or {}).get("verifiedStatus") == "valid":
        flags.append("verified")
    known_count = (profile.get("knownFollowers") or {}).get("count")
    if known_count:
        flags.append(f"known:{known_count}")
    return ",".join(flags) or "-"


def _render_raw_profile_row(profile: dict) -> str:
    handle = profile.get("handle") or profile.get("did") or "?"
    display = _sanitize_for_row((profile.get("displayName") or "").strip()) or "-"
    description = _truncate(
        _sanitize_for_row((profile.get("description") or "").strip()), 180
    ) or "-"
    return (
        f"@{handle} | {display} | {_raw_profile_flags(profile)} | "
        f"{profile.get('followersCount', '?')} followers | {profile.get('did', '?')} | {description}"
    )


@mcp.tool(
    output_schema=None,
    annotations={"title": "Read a list and its members", **_ANN_READ_ONLY},
)
async def get_list(
    list_ref: str,
    limit: Annotated[int, Field(ge=1, le=5000)] = 100,
    ctx: Context = None,
) -> str:
    """Read a list by saved name or AT URI and return its members.

    limit is the maximum member count to return (1-5000). Each member row is:
    handle | display name | relationship flags | follower count | DID | description.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_list_impl, list_ref, limit)


def _get_list_impl(list_ref: str, limit: int) -> str:
    if not 1 <= limit <= 5000:
        raise ValueError("limit must be between 1 and 5000.")
    uri = resolve_list_uri(list_ref)
    members: list[dict] = []
    cursor = None
    list_view = None
    while len(members) < limit:
        params = {"list": uri, "limit": min(100, limit - len(members))}
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get("app.bsky.graph.getList", params)
        list_view = list_view or data.get("list") or {}
        page = data.get("items") or []
        if not page:
            break
        members.extend((item.get("subject") or {}) for item in page)
        next_cursor = data.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    view = list_view or {}
    name = view.get("name") or list_ref
    purpose = (view.get("purpose") or "").split("#")[-1] or "?"
    count = view.get("listItemCount")
    header = f"{name} | {purpose} | {count if count is not None else '?'} members | {uri}"
    description = (view.get("description") or "").strip()
    if description:
        header += "\n" + description
    if not members:
        return header + "\n\nNo members."
    capped = " (limited)" if count is not None and len(members) < count else ""
    rows = "\n".join(_render_raw_profile_row(profile) for profile in members)
    return f"{header}\n\n{len(members)} returned{capped}\n{rows}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Create a Bluesky list", **_ANN_WRITE_NEW},
)
def create_list(
    name: str,
    description: str | None = None,
    purpose: Literal["curation", "moderation", "reference"] = "curation",
) -> str:
    """Create an empty list owned by the authenticated account.

    purpose is "curation" for a feed list, "moderation" for a mute/block list,
    or "reference" for uses such as starter packs. The purpose cannot be
    changed later; create a replacement list if needed.
    """
    name, description = _validate_list_text(name, description)
    record: dict = {
        "$type": _LIST_COLLECTION,
        "purpose": _LIST_PURPOSES[purpose],
        "name": name,
        "createdAt": _utc_now_iso(),
    }
    if description:
        record["description"] = description
        facets = _detect_facets(description)
        if facets:
            record["descriptionFacets"] = [_model_json(facet) for facet in facets]
    result = _xrpc_post(
        "com.atproto.repo.createRecord",
        {"repo": client.me.did, "collection": _LIST_COLLECTION, "record": record},
    )
    uri = result["uri"]
    cache_key = _cache_owned_list(name, uri)
    return f"List created.\nName: {name}\nCache key: {cache_key}\nURI: {uri}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Edit a Bluesky list", **_ANN_DESTRUCTIVE},
)
def update_list(
    list_ref: str,
    name: str | None = None,
    description: str | None = None,
) -> str:
    """Rename an owned list and/or replace its description.

    Pass description="" to clear it. Omitted fields are preserved. List purpose
    cannot be changed by the protocol; create a replacement list instead.
    """
    if name is None and description is None:
        raise ValueError("Provide name and/or description to update.")
    uri, rkey, current = _owned_list_record(list_ref)
    record = copy.deepcopy(current.get("value") or {})
    next_name, next_description = _validate_list_text(
        name if name is not None else str(record.get("name") or ""),
        description,
    )
    if name is not None:
        record["name"] = next_name
    if description is not None:
        if next_description:
            record["description"] = next_description
            facets = _detect_facets(next_description)
            if facets:
                record["descriptionFacets"] = [_model_json(facet) for facet in facets]
            else:
                record.pop("descriptionFacets", None)
        else:
            record.pop("description", None)
            record.pop("descriptionFacets", None)
    _xrpc_post(
        "com.atproto.repo.putRecord",
        {
            "repo": client.me.did,
            "collection": _LIST_COLLECTION,
            "rkey": rkey,
            "record": record,
            "swapRecord": current.get("cid"),
        },
    )
    final_name = str(record.get("name") or next_name)
    cache_key = _cache_owned_list(final_name, uri)
    return f"List updated.\nName: {final_name}\nCache key: {cache_key}\nURI: {uri}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Add members to a Bluesky list", **_ANN_WRITE_IDEMPOTENT},
)
async def add_list_members(
    list_ref: str,
    actors: list[str] | str,
    ctx: Context = None,
) -> str:
    """Batch-add handles or DIDs to an owned list.

    actors accepts an array or comma-separated string. Inputs are resolved to
    DIDs, duplicate inputs and existing members are skipped, and failures are
    reported without discarding successful additions.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_add_list_members_impl, list_ref, actors)


def _add_list_members_impl(list_ref, actors) -> str:
    uri, _rkey, _current = _owned_list_record(list_ref)
    resolved, failed, duplicates = _resolve_actor_inputs(actors)
    index = _listitem_index()
    new_members = [profile for profile in resolved if (uri, profile["did"]) not in index]
    existing = [profile["handle"] for profile in resolved if (uri, profile["did"]) in index]
    writes = [
        {
            "$type": "com.atproto.repo.applyWrites#create",
            "collection": _LISTITEM_COLLECTION,
            "value": {
                "$type": _LISTITEM_COLLECTION,
                "subject": profile["did"],
                "list": uri,
                "createdAt": _utc_now_iso(),
            },
        }
        for profile in new_members
    ]
    applied, error = _apply_writes_in_chunks(writes)
    added = [profile["handle"] for profile in new_members[:applied]]
    if error:
        failed.extend(f"{p['input']} ({error})" for p in new_members[applied:])
    lines = [f"List: {uri}", f"Added {len(added)}: {' '.join(added) if added else '-'}"]
    if existing:
        lines.append(f"Already members: {' '.join(existing)}")
    if duplicates:
        lines.append(f"Duplicate inputs skipped: {' '.join(duplicates)}")
    if failed:
        lines.append(f"Failed: {'; '.join(failed)}")
    return "\n".join(lines)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Remove members from a Bluesky list", **_ANN_DESTRUCTIVE},
)
async def remove_list_members(
    list_ref: str,
    actors: list[str] | str,
    ctx: Context = None,
) -> str:
    """Batch-remove handles or DIDs from an owned list.

    Every underlying membership record is deleted, including accidental
    duplicates. A DID is used as given, so deactivated or deleted accounts can
    still be removed. Accounts that are not members and inputs that do not
    resolve are reported separately.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_remove_list_members_impl, list_ref, actors)


def _remove_list_members_impl(list_ref, actors) -> str:
    uri, _rkey, _current = _owned_list_record(list_ref)
    resolved, failed, duplicates = _resolve_removal_inputs(actors)
    index = _listitem_index()
    targets: list[tuple[dict, str]] = []
    missing = []
    for profile in resolved:
        record_uris = index.get((uri, profile["did"])) or []
        if not record_uris:
            missing.append(profile["handle"])
        for record_uri in record_uris:
            targets.append((profile, record_uri))
    writes = []
    for _profile, record_uri in targets:
        _repo, _collection, rkey = _record_uri_parts(record_uri, _LISTITEM_COLLECTION)
        writes.append(
            {
                "$type": "com.atproto.repo.applyWrites#delete",
                "collection": _LISTITEM_COLLECTION,
                "rkey": rkey,
            }
        )
    applied, error = _apply_writes_in_chunks(writes)
    removed_handles = list(dict.fromkeys(profile["handle"] for profile, _ in targets[:applied]))
    if error:
        failed.append(f"remaining membership records ({error})")
    lines = [f"List: {uri}", f"Removed {len(removed_handles)}: {' '.join(removed_handles) if removed_handles else '-'}"]
    if applied > len(removed_handles):
        lines.append(f"Deleted {applied} membership records (duplicates were present).")
    if missing:
        lines.append(f"Not members: {' '.join(missing)}")
    if duplicates:
        lines.append(f"Duplicate inputs skipped: {' '.join(duplicates)}")
    if failed:
        lines.append(f"Failed: {'; '.join(failed)}")
    return "\n".join(lines)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Delete a Bluesky list", **_ANN_DESTRUCTIVE},
)
async def delete_list(
    list_ref: str,
    ctx: Context = None,
) -> str:
    """Delete an owned list and all of its membership records.

    The list record is deleted only after every batch of member deletions has
    succeeded. References from saved-feed preferences or starter packs are
    reported so the caller can clean those up separately.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_delete_list_impl, list_ref)


def _delete_list_impl(list_ref: str) -> str:
    uri, rkey, current = _owned_list_record(list_ref)
    refs = []
    try:
        if uri in json.dumps(_get_raw_preferences()):
            refs.append("saved-feed preferences")
    except Exception:
        pass
    try:
        packs, complete = _raw_repo_records("app.bsky.graph.starterpack", 5000)
        if any(uri in json.dumps(record.get("value") or {}) for record in packs):
            refs.append("starter packs")
        elif not complete:
            refs.append("possibly starter packs (not all were checked)")
    except Exception:
        pass

    membership_uris = [
        record_uri
        for (list_uri, _did), record_uris in _listitem_index().items()
        if list_uri == uri
        for record_uri in record_uris
    ]
    writes = []
    for record_uri in membership_uris:
        _repo, _collection, item_rkey = _record_uri_parts(record_uri, _LISTITEM_COLLECTION)
        writes.append(
            {
                "$type": "com.atproto.repo.applyWrites#delete",
                "collection": _LISTITEM_COLLECTION,
                "rkey": item_rkey,
            }
        )
    applied, error = _apply_writes_in_chunks(writes)
    if error:
        raise ValueError(
            f"Deleted {applied}/{len(writes)} membership records, then stopped: {error}. "
            "The list itself was not deleted; retry delete_list to finish safely."
        )
    _xrpc_post(
        "com.atproto.repo.deleteRecord",
        {
            "repo": client.me.did,
            "collection": _LIST_COLLECTION,
            "rkey": rkey,
            "swapRecord": current.get("cid"),
        },
    )
    _remove_cached_list(uri)
    warning = f"\nWarning: it was referenced by {', '.join(refs)}." if refs else ""
    return f"List deleted: {uri}\nDeleted {applied} membership records.{warning}"


# --- Mutuals tools ---


def _brief_error(exc: Exception) -> str:
    """One-line reason from an atproto failure.

    str() on these carries the entire Response — every HTTP header included —
    which is ~300 tokens of noise for a mistyped handle.
    """
    for obj in (exc, *getattr(exc, "args", ())):
        content = getattr(obj, "content", None)
        message = getattr(content, "message", None) or getattr(content, "error", None)
        if message:
            return str(message)
    return str(exc).split("\n")[0][:120]


def _format_mutuals_roster() -> str:
    roster = _db_list_mutuals()
    if not roster:
        return "No mutuals saved. add_mutuals(handles=[...]) to start one."
    lines = [f"{len(roster)} mutuals:"]
    for _did, handle, note, added_at in roster:
        added = datetime.fromtimestamp(added_at, _TZ_LOCAL).strftime("%Y-%m-%d")
        lines.append(f"  {handle}" + (f" | {note}" if note else "") + f" | added {added}")
    return "\n".join(lines)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Add accounts to the mutuals roster", **_ANN_WRITE_NEW},
)
async def add_mutuals(
    handles: list[str] | str,
    note: str | None = None,
    ctx: Context = None,
) -> str:
    """Add accounts to the saved mutuals roster.

    Mutuals are the accounts that matter to the user personally. get_posts
    names the ones present in whatever it fetched, in both raw and index mode,
    so they can be told apart from everyone else in the window — and in index
    mode, whose entries are topics rather than authors, that line is the only
    way to know they were there at all.

    handles: handles or DIDs, as a list or a comma-separated string. Each is
    resolved to a DID so the entry survives a handle change; unresolvable ones
    are reported and skipped. note is an optional label applied to all of them.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_add_mutuals_impl, handles, note)


def _add_mutuals_impl(handles, note) -> str:
    wanted = _v2_list(handles, strip_prefix="@")
    if not wanted:
        raise ToolError("No handles given.")
    added, updated, failed = [], [], []
    for handle in wanted:
        try:
            did = _resolve_did(handle)
        except Exception as e:
            failed.append(f"{handle} ({_brief_error(e)})")
            continue
        # Store the canonical handle, not whatever spelling was passed in.
        try:
            profile = client.app.bsky.actor.get_profile({"actor": did})
            canonical = profile.handle
        except Exception:
            canonical = handle
        (added if _db_add_mutual(did, canonical, note) else updated).append(canonical)

    parts = []
    if added:
        parts.append(f"Added: {' '.join(added)}")
    if updated:
        parts.append(f"Already saved (refreshed): {' '.join(updated)}")
    if failed:
        parts.append(f"Could not resolve: {'; '.join(failed)}")
    return "\n".join(parts + ["", _format_mutuals_roster()])


@mcp.tool(
    output_schema=None,
    annotations={"title": "List the mutuals roster", **_ANN_READ_ONLY},
)
def list_mutuals() -> str:
    """Show the saved mutuals roster."""
    return _format_mutuals_roster()


@mcp.tool(
    output_schema=None,
    annotations={"title": "Remove accounts from the mutuals roster", **_ANN_DESTRUCTIVE},
)
def remove_mutuals(handles: list[str] | str) -> str:
    """Remove accounts from the saved mutuals roster, by handle or DID."""
    wanted = _v2_list(handles, strip_prefix="@")
    if not wanted:
        raise ToolError("No handles given.")
    removed, missing = [], []
    for w in wanted:
        gone = _db_remove_mutual(w)
        (removed.append(gone) if gone else missing.append(w))
    parts = []
    if removed:
        parts.append(f"Removed: {' '.join(removed)}")
    if missing:
        parts.append(f"Not in the roster: {' '.join(missing)}")
    return "\n".join(parts + ["", _format_mutuals_roster()])


_QuoteStrategy = Literal["none", "author_only", "truncated_120", "full_when_parent_text_short", "full"]
_FeedMode = Literal["raw", "index"]

# Parameters shared by the feed-shaped tools. The description travels with the
# type into each tool's schema, so the scoping rules read the same everywhere
# without a copy in the server instructions.
SinceMinutes = Annotated[int | None, Field(
    ge=1,
    description="Time window: the last N minutes. With limit too, the newest "
                "N inside the window.",
)]
FeedLimit = Annotated[int | None, Field(
    ge=1,
    description="Cap to the newest N posts (best for algorithmic feeds, whose "
                "time order is interleaved). With neither limit nor "
                "since_minutes: the newest 50.",
)]
MinChars = Annotated[int, Field(
    ge=0, description="Drop posts with less text than this; link-card posts are exempt.",
)]
# A list of strings, or one comma-separated string: clients and models send
# both, and _v2_list normalizes either. No Field description, so each
# parameter keeps its own Args: entry.
StrList = list[str] | str | None
QuoteMode = Annotated[_QuoteStrategy, Field(
    description="How much of each quoted post the 'quotes:' table shows.",
)]


def _fetch_scope(fetcher, limit, since_minutes, min_chars):
    """Fetch the scoped candidate set for a feed-shaped tool.

    since_minutes bounds the window (all of it if limit is unset); limit caps
    the result to the newest N eligible posts. A fetcher marked
    `chronological` may end its window at the first all-old page (see
    fetch_by_time). Returns (items, effective_max_posts).
    """
    if since_minutes is not None:
        items = fetch_by_time(fetcher, since_minutes, max_items=limit, min_chars=min_chars,
                              ordered=getattr(fetcher, "chronological", False))
        return items, limit
    count = limit if limit is not None else 50
    return fetch_by_count(fetcher, count, min_chars=min_chars), count


def _fetch_and_render(fetcher, limit, since_minutes, min_chars, quote, **render_kw):
    """Shared scope-fetch + compact-row render path for feed-shaped tools."""
    items, max_posts = _fetch_scope(fetcher, limit, since_minutes, min_chars)
    return _render_compact_feed(items, since_minutes, quote, min_chars, max_posts=max_posts, **render_kw)


def _resolve_source(source: str):
    """Map a get_posts source to a fetcher closure.

    "timeline" (or "home"/"following") → the home timeline; otherwise a saved
    feed name, a list name (both via the name→URI caches, refreshed on miss),
    or a raw at:// URI dispatched on its collection segment.
    """
    if source.lower() in ("timeline", "home", "following"):
        def fetcher(cursor=None, limit=100):
            params = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            return client.app.bsky.feed.get_timeline(params)
        fetcher.chronological = True
        return fetcher

    if source.startswith("at://"):
        uri = source
        kind = "list" if "/app.bsky.graph.list/" in uri else "feed"
    else:
        key = source.lower()
        feed_gen, list_gen = _cache_gen["feed"], _cache_gen["list"]
        if key not in feed_cache and key not in list_cache:
            _refresh_on_miss("feed", feed_gen)
            _refresh_on_miss("list", list_gen)
        if key in feed_cache:
            uri, kind = feed_cache[key], "feed"
        elif key in list_cache:
            uri, kind = list_cache[key], "list"
        else:
            feeds = ", ".join(sorted(feed_cache.keys())) or "(none)"
            lists = ", ".join(sorted(list_cache.keys())) or "(none)"
            raise ValueError(
                f"Source '{source}' is not 'timeline', a saved feed, or a list "
                f"(see list_sources). Feeds: {feeds}. Lists: {lists}."
            )

    if kind == "list":
        def fetcher(cursor=None, limit=100):
            params = {"list": uri, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            return client.app.bsky.feed.get_list_feed(params)
        fetcher.chronological = True  # list feeds are reverse-chronological
    else:
        # Feed generators rank however they like: never chronological.
        def fetcher(cursor=None, limit=100):
            params = {"feed": uri, "limit": limit}
            if cursor:
                params["cursor"] = cursor
            return client.app.bsky.feed.get_feed(params)
    return fetcher


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch posts from timeline, feed, or list", **_ANN_READ_ONLY},
)
async def get_posts(
    since_minutes: SinceMinutes = None,
    limit: FeedLimit = None,
    source: str = "timeline",
    mode: _FeedMode = "raw",
    budget: Annotated[int, Field(ge=1, le=20000)] = 400,
    min_chars: MinChars = 10,
    quote: QuoteMode = "truncated_120",
    ctx: Context = None,
) -> str:
    """Fetch posts from the home timeline, a saved feed, or a list.

    Raw mode renders every post in scope, one row each; output past the size
    cap ends with a count of the posts not shown. Index mode renders the same
    scope as a topic map: entries ordered from most to least discussed, each a
    set of search terms, not a summary. To understand one, call search_posts
    with those terms and the time bounds and scope given in the index header,
    then get_thread on a useful hit. Keep all terms for a focused search, drop
    trailing terms to widen it, or add words to narrow it; single-word entries
    are ambiguous until searched. The index names topics, not authors, so its
    'mutuals here' line is the only author signal: read those people with
    search_posts(authors=[...]). min_chars and quote apply to raw mode only.

    Args:
        source: "timeline" (default), a feed or list name from list_sources,
            or an at:// URI.
        mode: "raw" to read the posts themselves, "index" to discover and
            prioritize the discussions in a window too large to read.
        budget: Index mode only. Raising it only adds lower-ranked topics; it
            does not reorder or expand existing entries.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _get_posts_impl, source, since_minutes, limit, mode, budget, min_chars, quote
        )


def _get_posts_impl(source, since_minutes, limit, mode, budget, min_chars, quote):
    fetcher = _resolve_source(source)
    if (mode or "raw").lower() == "index":
        # The index's own gates handle noise, and short replies carry the
        # thread structure it merges discussions on — never pre-filter here.
        items, max_posts = _fetch_scope(fetcher, limit, since_minutes, 0)
        return _render_index(items, source, budget, max_posts=max_posts)
    return _fetch_and_render(fetcher, limit, since_minutes, min_chars, quote)


def _lookup_post_view(post_id: int):
    """Resolve a numeric post ID to its PostView.

    An expired or unknown ID is a tool error, not a result: the caller has to
    re-fetch the feed to get a fresh ID, and reporting it as a normal result
    reads as "this post says: not found".
    """
    at_uri = _db_lookup_post(post_id)
    if not at_uri:
        raise ToolError(
            f"Post ID {post_id} is unknown or expired (IDs are valid for 1 hour). "
            "Re-run the feed or search that produced it to get a fresh ID."
        )
    resp = client.app.bsky.feed.get_posts({"uris": [at_uri]})
    if not resp.posts:
        raise ToolError(f"Post {post_id} is no longer available on Bluesky (deleted or blocked): {at_uri}")
    return resp.posts[0]


@mcp.tool(
    output_schema=None,
    annotations={"title": "Get a post by feed ID", **_ANN_READ_ONLY},
)
def get_post_by_id(post_id: int) -> str:
    """Fetch full details for a post by its numeric ID from a recent feed fetch.

    Includes alt text, link-card metadata, and image CDN URLs; use
    get_post_images to see the images themselves.

    Args:
        post_id: Numeric ID from get_posts, get_author_posts, search_posts,
            get_my_likes, or get_bookmarks output (valid 1 hour).
    """
    post = _lookup_post_view(post_id)
    detail = format_post_text(post, image_urls=True)
    if _get_post_image_items(post):
        detail += f"\nUse get_post_images(post_id={post_id}) to inspect the attached images."
    return detail


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch images attached to a post", **_ANN_READ_ONLY},
)
async def get_post_images(
    post_id: int,
    size: Literal["fullsize", "thumbnail"] = "fullsize",
    ctx: Context = None,
):
    """Fetch the actual images attached to a post as multimodal MCP content.

    Use this after a feed-shaped tool returns a numeric post ID. The ID is
    valid for 1 hour. Returns all images attached directly to that post, in
    display order, with an informational text block before each image. Handles
    both legacy four-image embeds and newer galleries (currently up to 10 in
    authoring clients, with a schema ceiling of 20), including quote-with-media
    posts. It does not fetch images belonging to the quoted post itself.

    Args:
        post_id: Numeric ID from get_posts, search_posts, likes, or bookmarks.
        size: "fullsize" for the large CDN rendition, or "thumbnail" for a
            smaller and cheaper rendition. CDN fullsize may not be the exact
            originally uploaded blob.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_post_images_impl, post_id, size)


def _get_post_images_impl(post_id: int, size: str):
    post = _lookup_post_view(post_id)
    at_uri = post.uri
    items = _get_post_image_items(post)
    if not items:
        return f"Post ID {post_id} has no attached images."

    content = [TextContent(
        type="text",
        text=f"Post {post_id}: {len(items)} attached image(s), requested size={size}.",
    )]
    total_bytes = 0
    delivered = 0
    for index, item in enumerate(items, start=1):
        if size == "thumbnail":
            url = (getattr(item, "thumb", None)
                   or getattr(item, "thumbnail", None)
                   or getattr(item, "fullsize", None))
        else:
            url = (getattr(item, "fullsize", None)
                   or getattr(item, "thumb", None)
                   or getattr(item, "thumbnail", None))
        alt = (getattr(item, "alt", "") or "").strip()
        if not url:
            content.append(TextContent(
                type="text", text=f"Image {index}/{len(items)} unavailable: no CDN URL."
            ))
            continue
        try:
            data, mime_type = _download_post_image(url)
            if total_bytes + len(data) > _POST_IMAGES_TOTAL_MAX_BYTES:
                content.append(TextContent(
                    type="text",
                    text=(f"Image {index}/{len(items)} skipped: combined images exceed "
                          f"the {_POST_IMAGES_TOTAL_MAX_BYTES}-byte response limit."),
                ))
                break
            total_bytes += len(data)
            delivered += 1
            label = f"Image {index}/{len(items)} | {mime_type} | {len(data)} bytes"
            if alt:
                label += f" | Alt: {alt}"
            content.append(TextContent(type="text", text=label))
            content.append(ImageContent(
                type="image",
                data=base64.b64encode(data).decode("ascii"),
                mimeType=mime_type,
            ))
        except Exception as e:
            log.warning("Post image fetch failed for %s image %d: %s", at_uri, index, e)
            content.append(TextContent(
                type="text", text=f"Image {index}/{len(items)} could not be fetched: {e}"
            ))

    if delivered == 0:
        content.append(TextContent(type="text", text="No image bytes were returned."))
    return content


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch a thread around a post", **_ANN_READ_ONLY},
)
async def get_thread(
    uri: str,
    depth: Annotated[int, Field(ge=0, le=1000)] = 20,
    parent_height: Annotated[int, Field(ge=0, le=1000)] = 6,
    full: bool = False,
    ctx: Context = None,
) -> str:
    """Fetch a thread around a post, formatted as plain text.

    Args:
        uri: AT URI or bsky.app URL of the post.
        depth: Reply levels below the post to fetch. Default 20, maximum 1000.
        parent_height: Parent posts above the post to fetch. Default 6, maximum 1000.
        full: Fetch the entire thread from its root instead (up to 1000 reply
            levels); ignores depth/parent_height.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_thread_impl, uri, depth, parent_height, full)


def _get_thread_impl(uri, depth, parent_height, full):
    if full:
        return _get_full_thread_impl(uri)
    at_uri = normalize_uri(uri)
    resp = client.app.bsky.feed.get_post_thread(_thread_params(at_uri, depth, parent_height))
    return _format_thread_node_text(resp.thread)


def _get_full_thread_impl(uri):
    at_uri = normalize_uri(uri)
    seed = client.app.bsky.feed.get_post_thread(_thread_params(at_uri, depth=0, parent_height=1000))
    root_uri = _root_uri_from_thread_node(seed.thread)
    resp = client.app.bsky.feed.get_post_thread(_thread_params(root_uri, depth=1000, parent_height=0))
    return _format_thread_node_text(resp.thread)


def _join_thread_blocks(blocks) -> str:
    return f"\n{SEP_POST}\n".join(block.strip() for block in blocks if block and block.strip())


def _format_thread_parent_chain(node) -> str:
    """Format parent posts from the root down to the direct parent."""
    chain = []
    current = node
    while current and hasattr(current, "post"):
        chain.append(current)
        current = getattr(current, "parent", None)
    chain.reverse()
    return _join_thread_blocks(format_post_text(parent) for parent in chain)


def _thread_node_block(node, reply_parent=None) -> str:
    """One thread node as a text block (or a placeholder for a missing node)."""
    if getattr(node, "not_found", False):
        return "[Post not found]"
    if getattr(node, "blocked", False):
        return "[Post blocked]"
    if not hasattr(node, "post"):
        return "[Unknown thread node]"
    return format_post_text(node, reply_parent=reply_parent)


def _format_thread_node_text(node, include_parent_chain=True) -> str:
    """Format a thread as text: parent chain, the post, then its replies.

    Replies are walked depth-first with an explicit stack — full=True asks for
    1000 reply levels, deeper than Python's recursion limit — and emitted in
    the same preorder as before, joined by '---'. Output stops at the compact
    renderer's size budget with a count of the posts left out, because a long
    thread can exceed the client's tool-result limit on its own.
    """
    if not hasattr(node, "post"):
        return _thread_node_block(node)

    head: list[str] = []
    if include_parent_chain and getattr(node, "parent", None):
        parents = _format_thread_parent_chain(node.parent)
        if parents:
            head += [parents, "[CONTINUED IN THREAD]"]
    root_block = format_post_text(node)
    root_replies = list(getattr(node, "replies", None) or [])
    if root_replies:
        root_block += f"\n{SEP_POST}\nREPLIES ({len(root_replies)})"
    head.append(root_block)

    blocks: list[str] = []
    used = sum(_out_size(b) + len(SEP_POST) + 2 for b in head)
    omitted = 0
    stack = [(reply, node) for reply in reversed(root_replies)]
    while stack:
        current, parent = stack.pop()
        if omitted:
            omitted += 1  # past the cap: only count, so the output has no gaps
        else:
            block = _thread_node_block(current, reply_parent=parent)
            cost = _out_size(block) + len(SEP_POST) + 2
            if blocks and used + cost > _RAW_OUTPUT_CHAR_BUDGET:
                omitted = 1
            else:
                used += cost
                blocks.append(block)
        if hasattr(current, "post"):
            stack.extend((reply, current) for reply in reversed(getattr(current, "replies", None) or []))
    text = _join_thread_blocks(head + blocks)
    if omitted:
        text += (f"\n{SEP_POST}\n… {omitted} more repl{'ies' if omitted != 1 else 'y'} not shown "
                 "(output size cap); read a branch with get_thread(uri=bsky.app/profile/"
                 "<handle>/post/<rk>) of the last reply shown.")
    return text


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch notifications", **_ANN_READ_ONLY},
)
async def get_notifications(
    limit: Annotated[int | None, Field(ge=1, le=5000)] = None,
    since_minutes: SinceMinutes = None,
    reasons: list[NotificationReason] | None = None,
    ctx: Context = None,
) -> str:
    """Fetch recent notifications (mentions, replies, quotes, likes, reposts, follows).

    Args:
        limit: Newest N notifications. Default 50 when since_minutes is unset.
        reasons: Only these notification types; omit for all.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _get_notifications_impl, limit, since_minutes, reasons
        )


def _get_notifications_impl(limit, since_minutes, reasons):
    selected_reasons = _normalize_notification_reasons(reasons)
    cutoff = None
    if since_minutes is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

    _NOTIF_CAP = 5000
    results = []
    cursor = None
    prev_cursor = None
    if cutoff:
        max_fetch = min(limit, _NOTIF_CAP) if limit is not None else _NOTIF_CAP
    else:
        max_fetch = limit if limit is not None else 50
    while len(results) < max_fetch:
        batch_size = min(100, max_fetch - len(results))
        params = {"limit": batch_size}
        if selected_reasons:
            params["reasons"] = selected_reasons
        if cursor:
            params["cursor"] = cursor
        resp = client.app.bsky.notification.list_notifications(params)
        if not resp.notifications:
            break
        done = False
        for notif in resp.notifications:
            if cutoff:
                ts = _parse_ts(notif.indexed_at)
                if ts and ts < cutoff:
                    done = True
                    break
            results.append(notif)
            if len(results) >= max_fetch:
                done = True
                break
        cursor = resp.cursor if hasattr(resp, "cursor") else None
        if done or not cursor or cursor == prev_cursor:
            break
        prev_cursor = cursor

    if not results:
        return "No notifications."

    cap_note = f"\n(Results capped at {_NOTIF_CAP} — use a shorter since_minutes for more accuracy)" if len(results) >= _NOTIF_CAP else ""
    parts = []
    for notif in results:
        ts = _parse_ts(notif.indexed_at)
        ts_str = ts.strftime("%Y-%m-%d %H:%M UTC") if ts else "?"
        handle = notif.author.handle
        display = notif.author.display_name or handle

        reason = notif.reason
        text = ""
        if hasattr(notif, "record") and notif.record and hasattr(notif.record, "text"):
            text = _get_text(notif.record)

        notif_uri = notif.uri or ""
        rkey = notif_uri.split("/")[-1] if notif_uri else ""

        notif_line = f"{reason}: @{handle}"
        if display != handle:
            notif_line += f" ({display})"
        notif_line += f" | {ts_str}"
        if rkey and "app.bsky.feed.post" in notif_uri:
            notif_line += f" | rk:{rkey}"
        if text:
            notif_line += f'\n"{_truncate(text, 200)}"'
        parts.append(notif_line)

    reason_note = f" ({', '.join(selected_reasons)})" if selected_reasons else ""
    header = f"{len(results)} notifications{reason_note}{cap_note}\n{URL_TEMPLATE_NOTE}\n"
    return header + f"\n{SEP_POST}\n".join(parts)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Look up a Bluesky profile", **_ANN_READ_ONLY},
)
def get_profile(handle: str) -> str:
    """Look up a Bluesky profile by handle or DID."""
    resp = client.app.bsky.actor.get_profile({"actor": handle})
    display = resp.display_name or resp.handle
    lines = [f"@{resp.handle}"]
    if display != resp.handle:
        lines[0] += f" ({display})"
    lines.append(f"Followers: {resp.followers_count} | Following: {resp.follows_count} | Posts: {resp.posts_count}")
    if resp.description:
        lines.append(resp.description.strip())
    lines.append(f"https://bsky.app/profile/{resp.handle}")
    return "\n".join(lines)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Search or discover Bluesky accounts", **_ANN_READ_ONLY},
)
async def search_actors(
    query: str | None = None,
    limit: Annotated[int, Field(ge=1, le=1000)] = 25,
    ctx: Context = None,
) -> str:
    """Find Bluesky accounts by handle, display name, or profile text.

    With query, searches actors. With no query, returns Bluesky's account
    suggestions for the authenticated user. limit may be 1-1000. Rows include
    relationship flags and DIDs so another tool can act on selected accounts.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_search_actors_impl, query, limit)


def _search_actors_impl(query: str | None, limit: int) -> str:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000.")
    endpoint = "app.bsky.actor.searchActors" if query and query.strip() else "app.bsky.actor.getSuggestions"
    actors: list[dict] = []
    cursor = None
    while len(actors) < limit:
        params: dict = {"limit": min(100, limit - len(actors))}
        if endpoint.endswith("searchActors"):
            params["q"] = query.strip()
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get(endpoint, params)
        page = data.get("actors") or []
        if not page:
            break
        actors.extend(page)
        next_cursor = data.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    label = f'actors matching "{query.strip()}"' if endpoint.endswith("searchActors") else "suggested accounts"
    if not actors:
        return f"No {label} found."
    rows = "\n".join(_render_raw_profile_row(actor) for actor in actors)
    return f"{len(actors)} {label}\nhandle | display | relationship | followers | DID | description\n{rows}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Find known followers of an account", **_ANN_READ_ONLY},
)
async def get_known_followers(
    actor: str,
    limit: Annotated[int, Field(ge=1, le=1000)] = 100,
    ctx: Context = None,
) -> str:
    """Return people the authenticated user follows who also follow actor.

    This is social-graph context, not proof of identity or trust. The response
    includes the resolved subject and up to limit known followers (1-1000),
    with relationship flags and DIDs.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_known_followers_impl, actor, limit)


def _get_known_followers_impl(actor: str, limit: int) -> str:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000.")
    followers: list[dict] = []
    cursor = None
    subject = None
    while len(followers) < limit:
        params: dict = {"actor": actor.strip().lstrip("@"), "limit": min(100, limit - len(followers))}
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get("app.bsky.graph.getKnownFollowers", params)
        subject = subject or data.get("subject") or {}
        page = data.get("followers") or []
        if not page:
            break
        followers.extend(page)
        next_cursor = data.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    subject_handle = subject.get("handle") or actor.strip().lstrip("@") if subject else actor
    header = f"Known followers of @{subject_handle}: {len(followers)} returned"
    if subject and subject.get("did"):
        header += f"\nSubject DID: {subject['did']}"
    if not followers:
        return header + "\nNone found."
    roster = _mutuals_present_handles(
        (profile.get("did") or "", profile.get("handle") or "") for profile in followers
    )
    if roster:
        header += "\nmutuals here: " + " ".join(roster)
    rows = "\n".join(_render_raw_profile_row(profile) for profile in followers)
    return header + "\nhandle | display | relationship | followers | DID | description\n" + rows


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch an account's latest posts", **_ANN_READ_ONLY},
)
async def get_author_posts(
    actor: str,
    limit: Annotated[int, Field(ge=1, le=1000)] = 50,
    include_replies: bool = True,
    include_reposts: bool = False,
    min_chars: MinChars = 0,
    quote: QuoteMode = "truncated_120",
    ctx: Context = None,
) -> str:
    """Fetch the newest posts from one account without requiring a search query.

    Same compact rows and post IDs as get_posts.

    Args:
        actor: Handle or DID.
        limit: Newest N posts, 1-1000.
        include_reposts: Off by default so the result is what the account
            authored; on to mirror its public profile feed.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _get_author_posts_impl,
            actor,
            limit,
            include_replies,
            include_reposts,
            min_chars,
            quote,
        )


def _get_author_posts_impl(actor, limit, include_replies, include_reposts, min_chars, quote):
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000.")
    actor = actor.strip().lstrip("@")
    profile = client.app.bsky.actor.get_profile({"actor": actor})
    feed_filter = "posts_with_replies" if include_replies else "posts_no_replies"

    def fetcher(cursor=None, limit=100):
        scan_cursor = cursor
        # A repost-heavy account can yield an empty authored-post page after
        # client-side repost filtering. Scan through several pages here so the
        # generic paginator does not mistake that for the end of the feed.
        for _ in range(10):
            params = {
                "actor": profile.did,
                "limit": limit,
                "filter": feed_filter,
                "include_pins": False,
            }
            if scan_cursor:
                params["cursor"] = scan_cursor
            resp = client.app.bsky.feed.get_author_feed(params)
            feed = list(resp.feed or [])
            if not include_reposts:
                feed = [item for item in feed if getattr(item, "reason", None) is None]
            next_cursor = getattr(resp, "cursor", None)
            if feed or not next_cursor or next_cursor == scan_cursor:
                return SimpleNamespace(feed=feed, cursor=next_cursor)
            scan_cursor = next_cursor
        return SimpleNamespace(feed=[], cursor=scan_cursor)

    rendered = _fetch_and_render(fetcher, limit, None, min_chars, quote, overflow_hint="lower limit")
    scope = "posts and replies" if include_replies else "posts without replies"
    if include_reposts:
        scope += ", including reposts"
    return f"Latest from @{profile.handle} ({scope})\n{rendered}"


# searchPostsV2 renamed the reverse-chronological sort "latest" -> "recent".
# "latest" is kept as an accepted alias so older call sites don't break.
_V2_SORTS = {"latest": "recent", "recent": "recent", "top": "top"}

# Params that satisfy the lexicon's "a query or at least one filter" rule.
# since/until/sort narrow a search but cannot stand on their own.
_V2_STANDALONE_FILTERS = (
    "authors", "excludeAuthors", "mentions", "hashtags", "excludeHashtags",
    "domains", "urls", "languages", "following", "hasMedia", "hasVideo",
    "threadRootUri", "excludeReplies", "repliesOnly",
)


def _v2_list(value, strip_prefix: str = "") -> list[str] | None:
    """Normalize a list-or-string param into a list of non-empty strings.

    MCP clients sometimes send a bare string (or a comma-separated one) where
    the schema asks for an array, so accept both shapes.
    """
    if value is None:
        return None
    items = value.split(",") if isinstance(value, str) else [str(v) for v in value]
    cleaned = []
    for item in items:
        item = item.strip()
        if strip_prefix:
            item = item.lstrip(strip_prefix)
        if item:
            cleaned.append(item)
    return cleaned or None


def _window_minutes_from_bounds(since_hours: float | None, since: str | None) -> int | None:
    """Minutes from now back to the search's lower bound, or None if unbounded.

    Feeds get this from `since_minutes`; search has to derive it. The renderer
    uses it only to compact timestamps, so an approximate value from an ISO
    `since` is good enough.
    """
    if since_hours:
        return max(1, round(since_hours * 60))
    if not since:
        return None
    try:
        dt = datetime.fromisoformat(since.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    minutes = int((datetime.now(timezone.utc) - dt).total_seconds() // 60)
    return minutes if minutes > 0 else None


def _run_v2_search(base: dict, limit: int, min_chars: int):
    """Paginate searchPostsV2.

    Returns (kept, filtered, raw_seen, hits_total, error). `raw_seen` counts
    posts the API actually matched, which distinguishes "the search found
    nothing" from "min_chars dropped everything".
    """
    kept = []
    filtered = 0
    raw_seen = 0
    cursor = None
    prev_cursor = None
    pages = 0
    hits_total = None
    max_pages = 20
    while len(kept) < limit and pages < max_pages:
        # When filtering, fetch full pages so we don't under-deliver.
        batch_size = 100 if min_chars > 0 else min(100, limit - len(kept))
        params = dict(base, limit=batch_size)
        if cursor:
            params["cursor"] = cursor
        try:
            data = _xrpc_get("app.bsky.feed.searchPostsV2", params)
        except httpx.HTTPStatusError as e:
            err = f"Search failed ({e.response.status_code}) — {_xrpc_error_message(e)}"
            return kept, filtered, raw_seen, hits_total, err
        pages += 1
        if hits_total is None:
            hits_total = data.get("hitsTotal")
        raw_posts = data.get("posts") or []
        if not raw_posts:
            break
        raw_seen += len(raw_posts)
        for raw_post in raw_posts:
            # searchPostsV2 is not in the atproto SDK yet, so hydrate the raw
            # JSON into the same PostView models the renderers expect.
            try:
                post = get_or_create(raw_post, models.AppBskyFeedDefs.PostView, strict=False)
            except Exception:
                log.warning("searchPostsV2: skipping unparsable post %s", raw_post.get("uri"))
                continue
            if min_chars > 0 and not _passes_min_chars(post, min_chars):
                filtered += 1
                continue
            kept.append(post)
            if len(kept) >= limit:
                break
        cursor = data.get("cursor")
        if not cursor or cursor == prev_cursor:
            break
        prev_cursor = cursor
    return kept, filtered, raw_seen, hits_total, None


def _xrpc_error_message(exc: httpx.HTTPStatusError) -> str:
    """Pull the XRPC error/message pair out of a failed response body."""
    try:
        body = exc.response.json()
    except Exception:
        return (exc.response.text or "").strip()[:200] or exc.response.reason_phrase
    name = body.get("error") or ""
    detail = body.get("message") or ""
    return f"{name}: {detail}".strip(": ") or str(body)[:200]


@mcp.tool(
    output_schema=None,
    annotations={"title": "Search Bluesky posts", **_ANN_READ_ONLY},
)
async def search_posts(
    query: str | None = None,
    limit: Annotated[int, Field(ge=1, le=2000)] = 25,
    sort: Literal["recent", "top"] = "recent",
    following: bool = False,
    authors: StrList = None,
    exclude_authors: StrList = None,
    mentions: StrList = None,
    hashtags: StrList = None,
    exclude_hashtags: StrList = None,
    domains: StrList = None,
    urls: StrList = None,
    languages: StrList = None,
    has_media: bool | None = None,
    has_video: bool | None = None,
    exclude_replies: bool = False,
    replies_only: bool = False,
    thread_root_uri: str | None = None,
    since_hours: Annotated[float | None, Field(gt=0)] = None,
    since: str | None = None,
    until: str | None = None,
    all_time: bool = False,
    min_chars: MinChars = 0,
    quote: QuoteMode = "truncated_120",
    ctx: Context = None,
) -> str:
    """Search Bluesky posts by text and/or structured filters.

    Supply a query, at least one filter, or both: following=True alone browses
    recent posts from followed accounts. A list filter ORs its values; different
    filters AND together; list filters take a list or a comma-separated
    string. Only a recent window (about a month) is searched
    unless all_time=True; a search that matches nothing there is retried once
    against the full index and the header says so, so an empty result means
    nothing matched. Same compact rows and post IDs as get_posts.

    Args:
        query: Search text; also how a get_posts(mode="index") entry is expanded.
        limit: Posts to return; pagination continues until this many pass
            min_chars.
        sort: "recent" (reverse-chronological) or "top" (by ranking).
        following: Only posts from accounts the authenticated user follows.
        authors: Handles or DIDs.
        exclude_authors: Handles or DIDs.
        mentions: Handles or DIDs mentioned in the post.
        hashtags: Tag names without the leading '#'.
        exclude_hashtags: Tag names without the leading '#'.
        domains: Only posts linking to these domains.
        urls: Only posts linking to these exact URLs.
        languages: Language codes, e.g. ["en", "fr"].
        has_media: Only posts carrying media.
        has_video: Only posts carrying video.
        exclude_replies: Mutually exclusive with replies_only.
        replies_only: Mutually exclusive with exclude_replies.
        thread_root_uri: Only posts inside the thread rooted at this post.
        since_hours: Relative window, the last N hours. Use this or since,
            not both.
        since: Absolute lower bound, ISO datetime or YYYY-MM-DD. since/until
            filter on indexing time, not creation time; they differ for
            backfilled posts.
        until: Absolute upper bound, ISO datetime or YYYY-MM-DD.
        all_time: Search the full index instead of the recent window.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _search_posts_impl, query, limit, sort, following,
            authors,
            exclude_authors, mentions, hashtags, exclude_hashtags, domains, urls,
            languages,
            has_media, has_video, exclude_replies, replies_only, thread_root_uri,
            since_hours, since, until, all_time, min_chars, quote,
        )


def _search_posts_impl(query, limit, sort, following, authors, exclude_authors,
                       mentions, hashtags, exclude_hashtags, domains, urls,
                       languages, has_media, has_video, exclude_replies,
                       replies_only, thread_root_uri, since_hours, since, until,
                       all_time, min_chars, quote):
    sort_v2 = _V2_SORTS.get((sort or "recent").strip().lower())
    if sort_v2 is None:
        raise ToolError(f'Invalid sort "{sort}" — use "recent" or "top".')
    if exclude_replies and replies_only:
        raise ToolError("exclude_replies and replies_only are mutually exclusive — pick one.")
    if since_hours is not None and since:
        raise ToolError("Use either since_hours (relative window) or since (absolute "
                        "timestamp), not both.")
    if since_hours is not None and since_hours <= 0:
        raise ToolError("since_hours must be a positive number of hours.")

    base: dict = {"sort": sort_v2}
    query = (query or "").strip()
    if query:
        base["query"] = query

    for key, value in (
        ("authors", _v2_list(authors)),
        ("excludeAuthors", _v2_list(exclude_authors)),
        ("mentions", _v2_list(mentions)),
        ("hashtags", _v2_list(hashtags, strip_prefix="#")),
        ("excludeHashtags", _v2_list(exclude_hashtags, strip_prefix="#")),
        ("domains", _v2_list(domains)),
        ("urls", _v2_list(urls)),
        ("languages", _v2_list(languages)),
    ):
        if value:
            base[key] = value

    # httpx renders Python bools as "True"/"False"; the lexicon wants "true"/"false".
    for key, flag in (
        ("following", following or None),
        ("hasMedia", has_media),
        ("hasVideo", has_video),
        ("excludeReplies", exclude_replies or None),
        ("repliesOnly", replies_only or None),
        ("allTime", all_time or None),
    ):
        if flag is not None:
            base[key] = "true" if flag else "false"

    if thread_root_uri:
        base["threadRootUri"] = normalize_uri(thread_root_uri)
    if since_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        base["since"] = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif since:
        base["since"] = since
    if until:
        base["until"] = until

    if not query and not any(k in base for k in _V2_STANDALONE_FILTERS):
        raise ToolError("Provide a `query` or at least one filter "
                        "(e.g. following=True, authors=[...], hashtags=[...]).")

    kept, filtered, raw_seen, hits_total, error = _run_v2_search(base, limit, min_chars)
    if error:
        raise ToolError(error)

    # The recent-post index only reaches back about a month. A search past it
    # returns an empty result rather than an error, which reads as "nobody said
    # anything" — retry once against the full index instead of reporting that
    # false negative. Skipped when min_chars did the filtering (raw_seen > 0),
    # since the search itself did match.
    expanded = False
    if not kept and not raw_seen and not all_time:
        retry_base = dict(base, allTime="true")
        r_kept, r_filtered, r_raw, r_hits, r_error = _run_v2_search(retry_base, limit, min_chars)
        if r_error:
            raise ToolError(r_error)
        if r_raw:
            kept, filtered, raw_seen, hits_total = r_kept, r_filtered, r_raw, r_hits
            base = retry_base
            expanded = True

    label = f'"{query}"' if query else "(filters only)"
    bits = [f"sort: {sort_v2}"]
    # since_hours is reported as the window the caller asked for, not as the
    # absolute timestamp it was converted into.
    skip = {"since"} if since_hours is not None else set()
    for key in _V2_STANDALONE_FILTERS + ("allTime", "since", "until"):
        value = base.get(key)
        if value is None or key in skip:
            continue
        bits.append(key if value == "true" else f"{key}={','.join(value) if isinstance(value, list) else value}")
    if since_hours is not None:
        bits.append(f"last {since_hours:g}h")
    if expanded:
        bits.append("auto-expanded past the recent index")

    if not kept:
        if raw_seen:
            return (f"No posts found for {label} · {' · '.join(bits)} — "
                    f"{raw_seen} matched but none reached min_chars={min_chars}")
        checked = "" if all_time else " (the full index was checked too)"
        return f"No posts found for {label} · {' · '.join(bits)}{checked}"

    if filtered:
        bits.append(f"{filtered} filtered")
    if hits_total:
        # hitsTotal saturates at 10000; report that as a floor, not an estimate.
        bits.append("10000+ hits" if hits_total >= 10000 else f"~{hits_total} hits")
    prefix = f"Search: {label} · {' · '.join(bits)}\n"
    window_minutes = _window_minutes_from_bounds(since_hours, base.get("since"))
    return prefix + _render_compact_feed(
        kept, window_minutes, quote, 0,
        overflow_hint="lower limit or tighten the time bound",
    )


def _resolve_reply_ref(reply_to_uri: str) -> models.AppBskyFeedPost.ReplyRef:
    """Build a reply ref from the parent post's own record.

    The parent's record.reply.root is authoritative — walking the thread via
    getPostThread breaks past the parentHeight cap on very deep threads and
    costs a much heavier call.
    """
    parent_uri = normalize_uri(reply_to_uri)
    resp = client.app.bsky.feed.get_posts({"uris": [parent_uri]})
    if not resp.posts:
        raise ValueError(f"Post to reply to was not found: {parent_uri}")
    parent = resp.posts[0]
    parent_ref = models.ComAtprotoRepoStrongRef.Main(uri=parent.uri, cid=parent.cid)
    record = getattr(parent, "record", None)
    reply = getattr(record, "reply", None) if record else None
    root = getattr(reply, "root", None) if reply else None
    if root is not None and getattr(root, "uri", None) and getattr(root, "cid", None):
        root_ref = models.ComAtprotoRepoStrongRef.Main(uri=root.uri, cid=root.cid)
    else:
        root_ref = parent_ref  # parent is itself the thread root
    return models.AppBskyFeedPost.ReplyRef(root=root_ref, parent=parent_ref)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Create a post or reply", **_ANN_WRITE_NEW},
)
async def create_post(
    text: str,
    reply_to_uri: str | None = None,
    embed_url: str | None = None,
    ctx: Context = None,
) -> str:
    """Create a new post or reply.

    URLs, @mentions, and #hashtags in the text are automatically converted to
    rich-text facets so they are clickable in Bluesky clients. A call identical
    to one that already published within the hour (same text, reply target
    and link) returns that post instead of publishing a duplicate.

    Args:
        text: Post text, 300 graphemes or fewer.
        reply_to_uri: AT URI or bsky.app URL of the post to reply to.
        embed_url: URL to embed as a link card (fetches OG title/description automatically).
    """
    async with _heartbeat(ctx):
        return await _run_tool(_create_post_impl, text, reply_to_uri, embed_url)


def _publish_key(*parts) -> str:
    """Stable identity of a publish call, for the retry journal."""
    return hashlib.sha256("\x1f".join(p or "" for p in parts).encode("utf-8")).hexdigest()


def _post_record_exists(uri: str) -> bool:
    """Ask the PDS (no AppView indexing lag) whether a post record exists."""
    repo, collection, rkey = _record_uri_parts(uri)
    try:
        _xrpc_get("com.atproto.repo.getRecord", {"repo": repo, "collection": collection, "rkey": rkey})
        return True
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (400, 404) and "RecordNotFound" in exc.response.text:
            return False
        raise


def _post_url(uri: str) -> str:
    return f"https://bsky.app/profile/{BSKY_HANDLE}/post/{uri.split('/')[-1]}"


def _create_post_impl(text, reply_to_uri, embed_url):
    _validate_post_text(text)

    reply_ref = _resolve_reply_ref(reply_to_uri) if reply_to_uri else None
    action = "Reply" if reply_to_uri else "Post"
    # A client that times out and retries would otherwise post twice: the
    # first call keeps running to completion after the client gives up.
    op_key = _publish_key("post", text, reply_ref.parent.uri if reply_ref else "", embed_url)
    earlier = _journal_lookup(op_key).get(0)
    if earlier and _post_record_exists(earlier[0]):
        return (f"{action} was already published by an identical earlier call; not posted again "
                f"(change the text to post it anyway).\nURI: {earlier[0]}\nURL: {_post_url(earlier[0])}")

    embed = _build_external_embed(embed_url) if embed_url else None
    facets = _detect_facets(text)

    _raise_if_cancelled("nothing was published")
    resp = client.send_post(text=text, reply_to=reply_ref, embed=embed, facets=facets)
    _journal_record(op_key, 0, resp.uri, resp.cid)
    return f"{action} created successfully.\nURI: {resp.uri}\nURL: {_post_url(resp.uri)}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Publish a multi-post thread", **_ANN_WRITE_NEW},
)
async def create_thread(text: str, ctx: Context = None) -> str:
    """Publish a multi-post thread in one call.

    Separate posts with a line containing only ---. Each post can embed a
    link card by placing [embed: https://...] on its own line within that post
    (the marker is stripped from the visible text). Every post must be 300
    graphemes or fewer after stripping the embed marker. URLs, @mentions, and
    #hashtags in each post are automatically converted to clickable facets.

    Example input:
        First post here, introducing the thread.

        ---

        Second post, continuing the thought.
        [embed: https://example.com/article]

        ---

        Third and final post.

    If an identical earlier call was interrupted part-way (a client timeout,
    say), its published posts are reused and only the rest are posted.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_create_thread_impl, text)


def _create_thread_impl(text: str) -> str:
    segments = [s.strip() for s in re.split(r"\n---\n|^---$", text, flags=re.MULTILINE)]
    segments = [s for s in segments if s]
    if not segments:
        raise ValueError("No posts found. Separate posts with --- on its own line.")

    # Validate all segments before publishing any, so validation errors never
    # leave a partial thread behind.
    parsed: list[tuple[str, str | None]] = []
    for i, segment in enumerate(segments):
        embed_match = _EMBED_RE.search(segment)
        embed_url = embed_match.group(1) if embed_match else None
        post_text = _EMBED_RE.sub("", segment).strip()
        _validate_post_text(post_text, label=f"Post {i + 1}")
        parsed.append((post_text, embed_url))

    # Journal each published post, so a retry of the same call after a client
    # timeout or cancellation resumes the chain instead of duplicating it.
    op_key = _publish_key("thread", *(f"{body}\x1e{url or ''}" for body, url in parsed))
    journal = _journal_lookup(op_key)
    published: list[tuple[str, str]] = []  # (uri, cid) in thread order
    while len(published) in journal and _post_record_exists(journal[len(published)][0]):
        published.append(journal[len(published)])
    if len(published) < len(journal):
        _journal_forget(op_key, len(published))  # deleted since; republish from there
    resumed = len(published)

    def urls() -> str:
        return "\n".join(f"  {i + 1}. {_post_url(uri)}" for i, (uri, _cid) in enumerate(published))

    for i in range(resumed, len(parsed)):
        post_text, embed_url = parsed[i]
        # Everything after the first post is inside the try: whatever fails,
        # the caller learns which posts are already live.
        try:
            embed = _build_external_embed(embed_url) if embed_url else None
            facets = _detect_facets(post_text)

            reply_ref = None
            if published:
                # Build reply ref directly from cached root/parent — no extra API call needed
                (root_uri, root_cid), (parent_uri, parent_cid) = published[0], published[-1]
                reply_ref = models.AppBskyFeedPost.ReplyRef(
                    root=models.ComAtprotoRepoStrongRef.Main(uri=root_uri, cid=root_cid),
                    parent=models.ComAtprotoRepoStrongRef.Main(uri=parent_uri, cid=parent_cid),
                )

            _raise_if_cancelled(f"thread stopped after {len(published)}/{len(parsed)} posts "
                                f"(a retry of the same call resumes it):\n{urls() or '  (none)'}")
            resp = client.send_post(text=post_text, reply_to=reply_ref, embed=embed, facets=facets)
        except _Cancelled:
            raise
        except Exception as e:
            raise ValueError(
                f"Thread publish failed at post {i + 1}/{len(parsed)}: {_brief_error(e)}\n"
                f"Posts already published (NOT rolled back; retrying the same call "
                f"continues from here):\n{urls() or '  (none)'}"
            ) from e
        published.append((resp.uri, resp.cid))
        _journal_record(op_key, i, resp.uri, resp.cid)

    if resumed == len(parsed):
        return f"Thread of {len(published)} posts was already published by an identical earlier call:\n{urls()}"
    note = f" (posts 1-{resumed} reused from an interrupted earlier call)" if resumed else ""
    return f"Thread of {len(published)} posts published{note}:\n{urls()}"


# --- Bookmarks ---


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch your bookmarks", **_ANN_READ_ONLY},
)
async def get_bookmarks(
    limit: Annotated[int, Field(ge=1, le=1000)] = 50,
    page: Annotated[int, Field(ge=1)] = 1,
    quote: QuoteMode = "truncated_120",
    ctx: Context = None,
) -> str:
    """Fetch bookmarked posts, most recently bookmarked first.

    The header reports the true total and page n/N (walk oldest-first by
    starting at the last page). Rows carry a bm:YY-MM-DD bookmark-date column;
    bookmarks of deleted/unavailable posts are listed with URIs for pruning
    via delete_bookmarks(uris=[...]).

    Args:
        limit: Page size.
        page: 1-based page number.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_bookmarks_impl, limit, page, quote)


# The full list is enumerated (100 per call) for an exact total and stable
# pages. Up to _BOOKMARK_ENUM_CAP is read on every call; a deeper page extends
# the scan as far as it needs, up to the hard cap. The result is kept for a
# couple of minutes so walking pages 1..N does not rescan N times, and so page
# boundaries hold still while the caller walks them; delete_bookmarks drops it.
_BOOKMARK_ENUM_CAP = 5000
_BOOKMARK_HARD_CAP = 50000
_BOOKMARK_SNAPSHOT_TTL = 120.0
_bookmark_snapshot: dict = {}  # empty = nothing loaded yet
_bookmark_lock = threading.Lock()


def _invalidate_bookmarks() -> None:
    with _bookmark_lock:
        _bookmark_snapshot.clear()


def _bookmark_records(need: int) -> tuple[list[tuple[str, datetime | None]], bool]:
    """(uri, bookmarked_at) newest first, deduplicated, and whether that is all of them."""
    with _bookmark_lock:
        snap = _bookmark_snapshot
        if not snap or time.monotonic() - snap["at"] > _BOOKMARK_SNAPSHOT_TTL:
            snap.clear()
            snap.update(at=time.monotonic(), records=[], seen=set(), cursor=None, complete=False)
        target = min(max(need, _BOOKMARK_ENUM_CAP), _BOOKMARK_HARD_CAP)
        while not snap["complete"] and len(snap["records"]) < target:
            _raise_if_cancelled()
            params: dict = {"limit": 100}
            if snap["cursor"]:
                params["cursor"] = snap["cursor"]
            data = _xrpc_get("app.bsky.bookmark.getBookmarks", params)
            raw = data.get("bookmarks", [])
            for bm in raw:
                uri = (bm.get("subject") or {}).get("uri", "")
                if uri and uri not in snap["seen"]:
                    snap["seen"].add(uri)
                    snap["records"].append((uri, _parse_ts(bm.get("createdAt"))))
            cursor = data.get("cursor")
            if not raw or not cursor or cursor == snap["cursor"]:
                snap["complete"] = True
            snap["cursor"] = cursor
        return list(snap["records"]), snap["complete"]


def _get_bookmarks_impl(limit, page, quote):
    limit = max(1, limit)
    page = max(1, page)

    records, complete = _bookmark_records(page * limit)
    if not records:
        return "No bookmarks found."
    enum_capped = not complete

    total = len(records)
    pages_total = (total + limit - 1) // limit
    total_label = f"{total}+" if enum_capped else str(total)
    start = (page - 1) * limit
    page_records = records[start : start + limit]
    if not page_records:
        beyond = (f" Only the newest {_BOOKMARK_HARD_CAP} bookmarks are reachable."
                  if enum_capped and total >= _BOOKMARK_HARD_CAP else "")
        return (
            f"Page {page} is out of range: {total_label} bookmarks "
            f"= {pages_total} page{'s' if pages_total != 1 else ''} of {limit}.{beyond}"
        )

    def _bm_date(dt: datetime | None) -> str:
        return dt.astimezone(_TZ_LOCAL).strftime("%y-%m-%d") if dt else "?"

    # Batch-fetch proper SDK post objects for this page only
    # (deleted/blocked posts drop out here).
    page_uris = [uri for uri, _ in page_records]
    post_map: dict[str, object] = {}
    for i in range(0, len(page_uris), 25):
        resp = client.app.bsky.feed.get_posts({"uris": page_uris[i : i + 25]})
        for p in resp.posts:
            post_map[p.uri] = p

    items = [post_map[uri] for uri in page_uris if uri in post_map]
    dead = [(uri, bm_dt) for uri, bm_dt in page_records if uri not in post_map]

    header_lines = [
        "Bookmarks · ordered by bookmark recency, newest first",
        f"page {page}/{pages_total} · bookmarks {start + 1}–{start + len(page_records)}"
        f" of {total_label} · bm: = bookmark date",
    ]
    if dead:
        header_lines.append(
            f"⚠ {len(dead)} bookmark{'s' if len(dead) != 1 else ''} on this page point"
            f" to deleted/unavailable posts — pass these URIs to delete_bookmarks to prune:"
        )
        header_lines.extend(
            f"  {uri} (bookmarked {_bm_date(bm_dt)})" for uri, bm_dt in dead
        )
    prefix = "\n".join(header_lines) + "\n"

    if not items:
        return prefix + "No accessible posts on this page."

    extra_cols = {uri: f"bm:{_bm_date(bm_dt)}" for uri, bm_dt in page_records}
    return prefix + _render_compact_feed(
        items, None, quote, 0, extra_cols=extra_cols,
        overflow_hint="use a smaller limit (page size)",
    )


@mcp.tool(
    output_schema=None,
    annotations={"title": "Delete bookmarks", **_ANN_DESTRUCTIVE},
)
async def delete_bookmarks(
    ids: list[int] | None = None,
    uris: list[str] | None = None,
    ctx: Context = None,
) -> str:
    """Delete one or more bookmarks in a single call.

    Accepts numeric post IDs from a recent get_bookmarks output (valid for
    1 hour), AT URIs, and bsky.app URLs — mix and match freely. Deletions
    are independent: a failure on one bookmark does not stop the rest, and
    the result reports the outcome per item.

    Args:
        ids: Numeric post IDs as shown in get_bookmarks output.
        uris: AT URIs (at://...) or bsky.app post URLs.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_delete_bookmarks_impl, ids or [], uris or [])


def _delete_bookmarks_impl(ids: list[int], uris: list[str]) -> str:
    if not ids and not uris:
        raise ToolError("Nothing to delete: provide ids and/or uris.")

    # Resolve every reference to an AT URI, keeping unresolvable ones for the report
    targets: list[str] = []
    unknown_ids: list[int] = []
    bad_uris: list[str] = []
    for pid in ids:
        at_uri = _db_lookup_post(pid)
        if at_uri is None:
            unknown_ids.append(pid)
        else:
            targets.append(at_uri)
    for uri in uris:
        try:
            targets.append(normalize_uri(uri))
        except Exception:
            bad_uris.append(uri)

    deleted: list[str] = []
    failed: list[tuple[str, str]] = []
    _invalidate_bookmarks()
    for at_uri in dict.fromkeys(targets):  # dedupe, preserve order
        _raise_if_cancelled(f"deleted {len(deleted)} bookmarks before stopping: {deleted}")
        try:
            _xrpc_post("app.bsky.bookmark.deleteBookmark", {"uri": at_uri})
            deleted.append(at_uri)
        except Exception as e:
            failed.append((at_uri, _brief_error(e)))

    lines = [f"Deleted {len(deleted)} bookmark{'s' if len(deleted) != 1 else ''}."]
    if failed:
        lines.append(f"Failed ({len(failed)}):")
        lines.extend(f"- {uri} — {err}" for uri, err in failed)
    if unknown_ids:
        lines.append(
            f"Unknown or expired post IDs (re-run get_bookmarks to refresh): "
            f"{', '.join(map(str, unknown_ids))}"
        )
    if bad_uris:
        lines.append("Unparseable URIs: " + ", ".join(bad_uris))
    return "\n".join(lines)


# --- Drafts ---


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch your drafts", **_ANN_READ_ONLY},
)
async def get_drafts(limit: Annotated[int, Field(ge=1, le=5000)] = 50, ctx: Context = None) -> str:
    """Fetch your saved drafts."""
    async with _heartbeat(ctx):
        return await _run_tool(_get_drafts_impl, limit)


def _get_drafts_impl(limit):
    drafts = []
    cursor = None
    while len(drafts) < limit:
        batch = min(100, limit - len(drafts))
        params: dict = {"limit": batch}
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get("app.bsky.draft.getDrafts", params)
        raw = data.get("drafts", [])
        if not raw:
            break
        drafts.extend(raw)
        cursor = data.get("cursor")
        if not cursor:
            break

    if not drafts:
        return "No drafts found."

    parts = []
    for dv in drafts:
        draft_id = dv.get("id", "?")
        created = _parse_ts(dv.get("createdAt"))
        updated = _parse_ts(dv.get("updatedAt"))
        created_str = created.strftime("%Y-%m-%d %H:%M UTC") if created else "?"
        updated_str = updated.strftime("%Y-%m-%d %H:%M UTC") if updated else "?"
        posts = dv.get("draft", {}).get("posts", [])
        draft_lines = [f"ID: {draft_id} | Created: {created_str} | Updated: {updated_str} | Posts: {len(posts)}"]
        for j, post in enumerate(posts, 1):
            text = post.get("text", "").strip()
            prefix = f"[{j}/{len(posts)}] " if len(posts) > 1 else ""
            draft_lines.append(f"{prefix}{text}")
        parts.append("\n".join(draft_lines))
    return f"{len(drafts)} drafts\n" + f"\n{SEP_POST}\n".join(parts)


def _parse_draft_posts(text: str) -> list[dict]:
    """Parse the create/update draft text convention into draft post inputs."""
    segments = [s.strip() for s in re.split(r"\n---\n|^---$", text, flags=re.MULTILINE)]
    segments = [s for s in segments if s]
    if not segments:
        raise ValueError("No posts found. Separate posts with --- on its own line.")

    draft_posts = []
    for i, segment in enumerate(segments):
        embed_match = _EMBED_RE.search(segment)
        embed_url = embed_match.group(1) if embed_match else None
        post_text = _EMBED_RE.sub("", segment).strip()
        length = grapheme.length(post_text)
        if length > 1000:
            raise ValueError(
                f"Post {i + 1} is {length} graphemes, exceeding the 1000 grapheme limit for drafts."
            )
        byte_len = len(post_text.encode("utf-8"))
        if byte_len > 10000:
            raise ValueError(
                f"Post {i + 1} is {byte_len} UTF-8 bytes, exceeding the 10000 byte draft limit."
            )
        post_obj: dict = {"text": post_text}
        if embed_url:
            post_obj["embedExternals"] = [{"uri": embed_url}]
        draft_posts.append(post_obj)
    return draft_posts


def _find_draft_raw(draft_id: str, cap: int = 5000) -> dict | None:
    """Find a draft by ID; updateDraft otherwise silently ignores unknown IDs."""
    cursor = None
    seen = 0
    while seen < cap:
        params: dict = {"limit": min(100, cap - seen)}
        if cursor:
            params["cursor"] = cursor
        data = _xrpc_get("app.bsky.draft.getDrafts", params)
        page = data.get("drafts") or []
        if not page:
            break
        for draft in page:
            if draft.get("id") == draft_id:
                return draft
        seen += len(page)
        next_cursor = data.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return None


@mcp.tool(
    output_schema=None,
    annotations={"title": "Save a draft post", **_ANN_WRITE_NEW},
)
def create_draft(text: str) -> str:
    """Save a draft without publishing. Supports multi-post thread drafts.

    Separate posts with a line containing only ---. Each post supports up to
    1000 graphemes (drafts allow longer text than published posts). Use
    [embed: https://...] on its own line to attach a link to a post.
    Note: link card previews (title/description) are resolved by the Bluesky app at publish time.

    Example:
        First post draft.

        ---

        Second post.
        [embed: https://example.com/article]
    """
    draft_posts = _parse_draft_posts(text)
    result = _xrpc_post("app.bsky.draft.createDraft", {"draft": {"posts": draft_posts}})
    draft_id = result.get("id", "?")
    return f"Draft saved. ID: {draft_id}  ({len(draft_posts)} post(s))"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Update a saved draft", **_ANN_DESTRUCTIVE},
)
async def update_draft(
    draft_id: str,
    text: str,
    preserve_embeds: bool = True,
    ctx: Context = None,
) -> str:
    """Replace the text/posts in an existing draft without publishing it.

    Uses the same --- thread separators and [embed: URL] markers as
    create_draft. By default, existing embed/media data is preserved by post
    position when the post count is unchanged; a new [embed] marker replaces
    that post's embed. To change the number of posts, set
    preserve_embeds=False explicitly, acknowledging that unmapped embeds and
    media will be dropped. Use the exact ID from get_drafts; it is checked
    before and after the write because the upstream endpoint silently ignores
    an unknown ID. Nothing is published.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _update_draft_impl, draft_id, text, preserve_embeds
        )


def _update_draft_impl(draft_id: str, text: str, preserve_embeds: bool) -> str:
    draft_id = draft_id.strip()
    if not draft_id:
        raise ValueError("draft_id cannot be empty.")
    current_view = _find_draft_raw(draft_id)
    if current_view is None:
        raise ValueError(f"Draft {draft_id} was not found; nothing was changed.")
    parsed_posts = _parse_draft_posts(text)
    current_draft = copy.deepcopy(current_view.get("draft") or {})
    current_posts = current_draft.get("posts") or []
    if preserve_embeds and len(parsed_posts) != len(current_posts):
        raise ValueError(
            f"Draft currently has {len(current_posts)} posts but the replacement has "
            f"{len(parsed_posts)}. Set preserve_embeds=False to change the post count "
            "and intentionally discard embeds/media that cannot be mapped."
        )

    if preserve_embeds:
        merged_posts = []
        for old_post, new_post in zip(current_posts, parsed_posts):
            merged = copy.deepcopy(old_post)
            merged["text"] = new_post["text"]
            if "embedExternals" in new_post:
                merged["embedExternals"] = new_post["embedExternals"]
            merged_posts.append(merged)
    else:
        merged_posts = parsed_posts
    current_draft["posts"] = merged_posts

    _xrpc_post(
        "app.bsky.draft.updateDraft",
        {"draft": {"id": draft_id, "draft": current_draft}},
    )
    verified = _find_draft_raw(draft_id)
    if verified is None:
        raise ValueError("Draft update returned without error, but the draft could not be re-read.")
    actual_texts = [post.get("text", "") for post in (verified.get("draft", {}).get("posts") or [])]
    expected_texts = [post.get("text", "") for post in merged_posts]
    if actual_texts != expected_texts:
        raise ValueError("Draft update returned without error, but verification did not match.")
    return f"Draft {draft_id} updated and verified. ({len(merged_posts)} post(s))"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Delete a draft", **_ANN_DESTRUCTIVE},
)
def delete_draft(draft_id: str) -> str:
    """Delete a draft by its ID (shown in get_drafts output)."""
    _xrpc_post("app.bsky.draft.deleteDraft", {"id": draft_id})
    return f"Draft {draft_id} deleted."


# --- Likes ---


@mcp.tool(
    output_schema=None,
    annotations={"title": "Fetch your liked posts", **_ANN_READ_ONLY},
)
async def get_my_likes(
    limit: FeedLimit = None,
    since_minutes: SinceMinutes = None,
    min_chars: MinChars = 20,
    quote: QuoteMode = "truncated_120",
    ctx: Context = None,
) -> str:
    """Fetch posts liked by the authenticated account, most recently liked first.

    Same compact rows as get_posts. The API exposes post creation time, not
    like time, so since_minutes filters by when the post was written: a
    recently liked old post won't appear in a windowed fetch.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _get_my_likes_impl, limit, since_minutes, min_chars, quote
        )


def _get_my_likes_impl(limit, since_minutes, min_chars, quote):
    def fetcher(cursor=None, limit=100):
        params = {"actor": BSKY_HANDLE, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return client.app.bsky.feed.get_actor_likes(params)

    return _fetch_and_render(
        fetcher, limit, since_minutes, min_chars, quote,
        overflow_hint="narrow since_minutes/limit",
    )


# --- Direct messages (chat) ---
# Bluesky DMs use the chat.bsky.convo.* lexicons behind the central chat
# service (see _dm()). A group chat is just a convo with kind="group", so the
# same list/read/send endpoints cover both 1:1 and group messages. Group
# *management* (create/members/join-links) is intentionally not exposed.


def _resolve_did(handle_or_did: str) -> str:
    """Resolve a handle (with or without leading @) to a DID; pass DIDs through."""
    h = handle_or_did.strip().lstrip("@")
    if h.startswith("did:"):
        return h
    try:
        return client.com.atproto.identity.resolve_handle({"handle": h}).did
    except Exception:
        return client.app.bsky.actor.get_profile({"actor": h}).did


def _member_map(convo) -> dict[str, str]:
    """did -> handle for the members of a convo (sender views only carry a DID)."""
    return {m.did: (m.handle or m.did) for m in (getattr(convo, "members", None) or [])}


def _is_group(convo) -> bool:
    return "group" in (getattr(getattr(convo, "kind", None), "py_type", "") or "").lower()


def _full_member_map(dm, convo, convo_id) -> dict[str, str]:
    """Complete did -> handle map. For groups, convo.members is only a preview
    subset, so page through get_convo_members to resolve every sender."""
    members = _member_map(convo)
    if not _is_group(convo):
        return members
    cursor = None
    prev = None
    for _ in range(20):  # cap: 20 pages * 100 = 2000 members (limit is 50)
        params = {"convo_id": convo_id, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            resp = dm.get_convo_members(params)
        except Exception:
            break
        for m in (resp.members or []):
            members[m.did] = m.handle or m.did
        cursor = getattr(resp, "cursor", None)
        if not cursor or cursor == prev:
            break
        prev = cursor
    return members


def _other_members(convo, my_did) -> list[str]:
    others = [f"@{m.handle}" for m in (convo.members or []) if m.did != my_did]
    return others or [f"@{m.handle}" for m in (convo.members or [])]


def _convo_label(convo, my_did) -> str:
    """Human label for a convo: a group's name + size, or the 1:1 member(s).

    ConvoView.kind is a discriminated object (DirectConvo | GroupConvo), not a
    string — groups carry name/member_count there.
    """
    kind = getattr(convo, "kind", None)
    pt = (getattr(kind, "py_type", "") or "").lower()
    if "group" in pt:
        name = getattr(kind, "name", None)
        count = getattr(kind, "member_count", None)
        label = f'group "{name}"' if name else "group"
        return f"{label} ({count} members)" if count else label
    return "direct " + ", ".join(_other_members(convo, my_did))


def _render_convo_row(convo, my_did) -> str:
    parts = [convo.id, _convo_label(convo, my_did)]
    if convo.unread_count:
        parts.append(f"unread {convo.unread_count}")
    if convo.muted:
        parts.append("muted")
    lm = getattr(convo, "last_message", None)
    if lm is not None:
        txt = getattr(lm, "text", None)
        preview = _truncate(_sanitize_for_row(txt), 80) if txt else "[non-text]"
        parts.append(f'last: "{preview}"')
    return " | ".join(parts)


def _dm_sender_label(msg, members, my_did) -> str:
    """Human sender label for a DM message or embedded reply."""
    sender_did = getattr(getattr(msg, "sender", None), "did", None)
    if sender_did == my_did:
        return "me"
    return "@" + members.get(sender_did, sender_did or "?")


def _dm_record_view(embed):
    """Return the inner record view carried by a record-style embed.

    A chat shared-post embed is app.bsky.embed.record#view. Posts can also
    carry recordWithMedia embeds, whose record view adds one wrapper. Unwrap
    only those containers; the returned viewRecord / unavailable variant is
    the value callers inspect.
    """
    node = embed
    for _ in range(2):
        pt = (getattr(node, "py_type", "") or "").lower()
        if pt.endswith("embed.record#view") or pt.endswith("embed.recordwithmedia#view"):
            child = getattr(node, "record", None)
            if child is None:
                break
            node = child
            continue
        # Attribute-only stand-ins keep the formatter easy to test and cover
        # forward-compatible SDK objects whose discriminator was not decoded.
        if not pt and getattr(node, "record", None) is not None:
            node = node.record
            continue
        break
    return node


def _format_dm_shared_post(embed, *, label: str = "shared post", include_embeds: bool = True) -> str | None:
    """Render a chat record embed without fetching or expanding a thread.

    The chat service already supplies a hydrated app.bsky.embed.record view.
    Render at most one nested level so a shared quote remains useful without
    turning a DM fetch into an unbounded post tree.
    """
    if embed is None:
        return None
    inner = _dm_record_view(embed)
    pt = (getattr(inner, "py_type", "") or "").lower()
    if "viewnotfound" in pt or "notfound" in pt:
        return "[shared post deleted]"
    if "viewblocked" in pt or "blocked" in pt:
        return "[shared post blocked]"
    if "viewdetached" in pt or "detached" in pt:
        return "[shared post detached]"

    author = getattr(inner, "author", None)
    if author is None:
        return None
    handle = getattr(author, "handle", None) or getattr(author, "did", None) or "?"
    value = getattr(inner, "value", None) or getattr(inner, "record", None)
    text = _get_text(value) if value is not None else ""
    uri = getattr(inner, "uri", None)
    heading = f"{label} | @{handle}" + (f" | uri:{uri}" if uri else "")
    lines = [heading, text or "[no text]"]

    if include_embeds:
        for nested in (getattr(inner, "embeds", None) or []):
            info = _get_embed_info(SimpleNamespace(embed=nested))
            if info:
                lines.append(info)
            nested_post = _format_dm_shared_post(
                nested, label="embedded post", include_embeds=False
            )
            if nested_post:
                lines.extend(nested_post.splitlines())
    return "\n".join(lines)


def _format_dm_reply_context(reply, members, my_did) -> str | None:
    """Render the single parent-message view included with a DM reply."""
    if reply is None:
        return None
    pt = (getattr(reply, "py_type", "") or "").lower()
    if "deletedmessage" in pt:
        return "reply to [deleted message]"
    if "messagebeforeuserjoinedgroup" in pt:
        return "reply to [message before joining group]"
    if "messageview" not in pt and getattr(reply, "sender", None) is None:
        return None

    who = _dm_sender_label(reply, members, my_did)
    msg_id = getattr(reply, "id", "")
    heading = f"reply to {who}" + (f" | id:{msg_id}" if msg_id else "")
    text = getattr(reply, "text", None)
    context_lines = (text if text else "[no text]").splitlines() or ["[no text]"]
    shared = _format_dm_shared_post(getattr(reply, "embed", None))
    if shared:
        context_lines.extend(shared.splitlines())
    return "\n".join([heading, *(f"> {line}" for line in context_lines)])


def _format_message_block(msg, members, my_did, is_unread: bool) -> str:
    pt = (getattr(msg, "py_type", "") or "").lower()
    ts = _parse_ts(getattr(msg, "sent_at", None))
    when = ts.astimezone(_TZ_LOCAL).strftime("%m-%d %H:%M") if ts else "?"
    flag = " [unread]" if is_unread else ""
    msg_id = getattr(msg, "id", "")

    if "systemmessage" in pt:  # group admin event (member added, group edited, ...)
        data = getattr(msg, "data", None)
        detail = getattr(data, "text", None) if data is not None else None
        body = f"[system] {detail}" if detail else "[system event]"
        return f"system | {when} | id:{msg_id}{flag}\n{body}"

    who = _dm_sender_label(msg, members, my_did)
    line = f"{who} | {when} | id:{msg_id}{flag}"
    if "deletedmessage" in pt:
        return f"{line}\n[deleted message]"
    text = getattr(msg, "text", None)
    lines = [line]
    reply = _format_dm_reply_context(getattr(msg, "reply_to", None), members, my_did)
    if reply:
        lines.extend(reply.splitlines())
    lines.append(text if text else "[no text]")
    shared = _format_dm_shared_post(getattr(msg, "embed", None))
    if shared:
        lines.extend(shared.splitlines())
    reactions = getattr(msg, "reactions", None)
    if reactions:
        counts: dict[str, int] = {}
        for r in reactions:
            counts[r.value] = counts.get(r.value, 0) + 1
        lines.append("reactions: " + " ".join(f"{v}x{n}" if n > 1 else v for v, n in counts.items()))
    return "\n".join(lines)


@mcp.tool(
    output_schema=None,
    annotations={"title": "List DM conversations", **_ANN_READ_ONLY},
)
async def get_conversations(
    unread_only: bool = False,
    kind: Literal["direct", "group"] | None = None,
    requests: bool = False,
    limit: Annotated[int, Field(ge=1, le=1000)] = 30,
    ctx: Context = None,
) -> str:
    """List direct-message conversations (1:1 DMs and group chats).

    Each row's convo_id is what get_messages / send_message / mark_read take.

    Args:
        unread_only: Only convos with unread messages.
        kind: "direct" (1:1) or "group"; omit for both.
        requests: Pending message requests from people the user doesn't
            follow, instead of accepted convos.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_get_conversations_impl, unread_only, kind, requests, limit)


def _get_conversations_impl(unread_only, kind, requests, limit):
    dm = _dm()
    my_did = client.me.did
    params = {}
    if unread_only:
        params["read_state"] = "unread"
    if kind:
        params["kind"] = kind
    if requests:
        params["status"] = "request"
    convos, more = _dm_list_convos(dm, params, max(limit, 1))
    if not convos:
        return "No message requests." if requests else "No conversations."
    label = "message requests" if requests else "conversations"
    rows = [_render_convo_row(c, my_did) for c in convos]
    legend = 'cols: convo_id | direct @members or group "name" (N members) | unread N | muted | last message'
    footer = f"\n… more {label} exist; raise limit (max 1000) to see older ones." if more else ""
    return f"{len(convos)} {label}\n{legend}\n" + "\n".join(rows) + footer


def _dm_list_convos(dm, params, limit: int):
    """Page through listConvos until `limit` convos. Returns (convos, more_exist)."""
    convos: list = []
    cursor = None
    while len(convos) < limit:
        _raise_if_cancelled()
        page_params = dict(params, limit=min(100, limit - len(convos)))
        if cursor:
            page_params["cursor"] = cursor
        resp = dm.list_convos(page_params)
        page = resp.convos or []
        convos.extend(page)
        next_cursor = getattr(resp, "cursor", None)
        if not page or not next_cursor or next_cursor == cursor:
            return convos, False
        cursor = next_cursor
    return convos[:limit], True


@mcp.tool(
    output_schema=None,
    annotations={"title": "Read DM messages", **_ANN_READ_ONLY},
)
async def get_messages(
    convo_id: str,
    unread_only: bool = True,
    context: Annotated[int, Field(ge=0, le=200)] = 3,
    limit: Annotated[int, Field(ge=1, le=1000)] = 30,
    since_minutes: Annotated[int | None, Field(ge=1)] = None,
    ctx: Context = None,
) -> str:
    """Read messages in a conversation (1:1 or group), oldest -> newest.

    By default returns the unread messages plus a few preceding ones, with the
    unread ones tagged [unread] below a '===== unread below =====' divider.
    Replies include one level of parent-message context; shared posts include
    their author, AT URI, text, and a compact description of their embed.

    Args:
        convo_id: From get_conversations.
        unread_only: False for the newest `limit` messages instead.
        context: Preceding messages shown before the unread ones.
        limit: Message count when unread_only=False.
        since_minutes: Time bound when unread_only=False.
    """
    async with _heartbeat(ctx):
        return await _run_tool(
            _get_messages_impl, convo_id, unread_only, context, limit, since_minutes
        )


def _get_messages_impl(convo_id, unread_only, context, limit, since_minutes):
    dm = _dm()
    convo = dm.get_convo({"convo_id": convo_id}).convo
    members = _full_member_map(dm, convo, convo_id)
    my_did = client.me.did
    unread = convo.unread_count or 0

    cutoff = None
    if since_minutes is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

    if unread_only:
        target = (unread + max(context, 0)) if unread else 10
    else:
        target = limit
    target = max(min(target, 1000), 1)

    # get_messages returns newest-first; paginate until we have `target`.
    collected = []
    cursor = None
    prev_cursor = None
    while len(collected) < target:
        params = {"convo_id": convo_id, "limit": min(100, target - len(collected))}
        if cursor:
            params["cursor"] = cursor
        resp = dm.get_messages(params)
        msgs = resp.messages or []
        if not msgs:
            break
        stop = False
        for m in msgs:
            if cutoff:
                ts = _parse_ts(getattr(m, "sent_at", None))
                if ts and ts < cutoff:
                    stop = True
                    break
            collected.append(m)
            if len(collected) >= target:
                stop = True
                break
        cursor = getattr(resp, "cursor", None)
        if stop or not cursor or cursor == prev_cursor:
            break
        prev_cursor = cursor

    if not collected:
        return "No messages."

    collected.reverse()  # -> chronological
    unread_in_view = unread if unread_only else 0
    unread_start = len(collected) - unread_in_view  # first unread index

    entries = []
    for i, m in enumerate(collected):
        if unread_in_view and i == unread_start:
            entries.append("===== unread below =====")
        entries.append(_format_message_block(m, members, my_did, unread_in_view and i >= unread_start))

    head = [f"convo {convo_id}", _convo_label(convo, my_did)]
    if unread:
        head.append(f"{unread} unread")
    header = " | ".join(head)
    if unread_only and not unread:
        header += "\n(no unread — showing recent messages)"
    return header + "\n" + f"\n{SEP_POST}\n".join(entries)


@mcp.tool(
    output_schema=None,
    annotations={"title": "Send a direct message", **_ANN_WRITE_NEW},
)
async def send_message(
    text: str,
    convo_id: str | None = None,
    to_handle: str | None = None,
    ctx: Context = None,
) -> str:
    """Send a direct message to a 1:1 or group conversation.

    Provide exactly one of convo_id (reply in an existing conversation, from
    get_conversations) or to_handle (start or reuse a 1:1 conversation with that
    handle/DID). URLs, @mentions, and #hashtags become clickable facets. Limit
    1000 graphemes.
    """
    async with _heartbeat(ctx):
        return await _run_tool(_send_message_impl, text, convo_id, to_handle)


def _send_message_impl(text, convo_id, to_handle):
    if bool(convo_id) == bool(to_handle):
        raise ValueError("Provide exactly one of convo_id or to_handle.")
    _validate_message_text(text)
    dm = _dm()
    if to_handle:
        did = _resolve_did(to_handle)
        convo = dm.get_convo_for_members(
            models.ChatBskyConvoGetConvoForMembers.Params(members=[did])
        ).convo
        convo_id = convo.id
    facets = _detect_facets(text)
    msg = dm.send_message(
        models.ChatBskyConvoSendMessage.Data(
            convo_id=convo_id,
            message=models.ChatBskyConvoDefs.MessageInput(text=text, facets=facets),
        )
    )
    return f"Message sent.\nconvo_id: {convo_id}\nmessage id: {msg.id}"


@mcp.tool(
    output_schema=None,
    annotations={"title": "Mark a conversation read", **_ANN_WRITE_NEW, "idempotentHint": True},
)
async def mark_read(
    convo_id: str,
    message_id: str | None = None,
    ctx: Context = None,
) -> str:
    """Mark a conversation read up to message_id, or the whole convo if omitted."""
    async with _heartbeat(ctx):
        return await _run_tool(_mark_read_impl, convo_id, message_id)


def _mark_read_impl(convo_id, message_id):
    dm = _dm()
    data = {"convo_id": convo_id}
    if message_id:
        data["message_id"] = message_id
    resp = dm.update_read(data)
    unread = getattr(getattr(resp, "convo", None), "unread_count", None)
    suffix = f" (unread now {unread})" if unread is not None else ""
    return f"Marked read: {convo_id}{suffix}"


# --- Accept-header fixup for Open WebUI compatibility ---

class FixAcceptHeaderMiddleware:
    """Open WebUI omits text/event-stream from Accept, causing FastMCP to 406.
    This injects it so spec-strict servers accept the request."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        # Only the MCP endpoint; the OAuth routes serve HTML/JSON to browsers.
        if scope["type"] == "http" and scope.get("path", "").startswith(MCP_PATH):
            headers = dict(scope.get("headers", []))
            accept = headers.get(b"accept", b"").decode()
            if "text/event-stream" not in accept:
                new_accept = f"{accept}, text/event-stream" if accept else "application/json, text/event-stream"
                scope["headers"] = [
                    (k, v) for k, v in scope["headers"] if k != b"accept"
                ] + [(b"accept", new_accept.encode())]
        await self.app(scope, receive, send)


# --- Entry point ---

def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _check_auth_exposure(modes: list[str]) -> None:
    """Refuse to serve DMs and write tools unauthenticated beyond loopback.

    A warning is not enough: an env file that lost its auth settings would
    otherwise publish the whole account on every interface. Set
    MCP_ALLOW_UNAUTHENTICATED=1 to accept that deliberately (e.g. behind an
    authenticating proxy on a private network).
    """
    if modes:
        log.info("Auth: %s. MCP endpoint: http://%s:%d%s", " + ".join(modes), MCP_HOST, MCP_PORT, MCP_PATH)
        return
    if _is_loopback_host(MCP_HOST):
        log.warning("No MCP_AUTH_TOKEN or GitHub OAuth configured — unauthenticated, "
                    "but bound to loopback (%s) only.", MCP_HOST)
        return
    if os.getenv("MCP_ALLOW_UNAUTHENTICATED", "").lower() in ("1", "true", "yes"):
        log.warning("No auth configured and bound to %s: anyone who can reach this port controls "
                    "the account (MCP_ALLOW_UNAUTHENTICATED is set).", MCP_HOST)
        return
    raise SystemExit(
        f"Refusing to start: no MCP_AUTH_TOKEN or GitHub OAuth is configured and MCP_HOST={MCP_HOST} "
        "is not loopback. Configure auth, bind to 127.0.0.1, or set MCP_ALLOW_UNAUTHENTICATED=1."
    )


def _startup() -> None:
    """Log in (bounded retries) and warm the name caches; neither failure is fatal.

    A Bluesky outage at boot used to crash the process and leave systemd
    re-running the whole login sequence. Now the server comes up regardless and
    _SessionGuard retries the login (with backoff) on the next tool call.
    """
    _require_credentials()
    for attempt in range(1, 4):
        try:
            _ensure_logged_in()
            break
        except Exception as exc:
            log.warning("Startup login attempt %d/3 failed: %s", attempt, _brief_error(exc))
            if attempt < 3:
                time.sleep(max(0.0, _login_state["retry_at"] - time.monotonic()) or 5.0)
    else:
        log.error("Could not log in at startup; tool calls will keep retrying.")
        return
    try:
        refresh_caches()
    except Exception as exc:
        log.warning("Feed/list names not loaded (%s); they load on first use.", _brief_error(exc))


if __name__ == "__main__":
    import uvicorn

    _check_auth_exposure(
        [m for m, on in (("header token", MCP_AUTH_TOKEN), ("GitHub OAuth", GITHUB_CLIENT_ID)) if on]
    )
    _startup()

    app = FixAcceptHeaderMiddleware(mcp.http_app(path=MCP_PATH))
    # ws="none": transport is streamable HTTP, so skip uvicorn's WebSocket
    # auto-detection — it imports the deprecated websockets.legacy stack for
    # a protocol this server never speaks.
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT, ws="none")
