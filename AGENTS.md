# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## What this is

A single-file Python MCP server (`server.py`) that exposes a Bluesky account to Codex via the Model Context Protocol. Codex can read timelines, feeds, lists, notifications, threads, and profiles, and can create posts/replies. It can also read and send direct messages — both 1:1 DMs and group chats — and mark conversations read. Likes, reposts, follows, DM *group management* (creating groups, managing members, join links), and media uploads are intentionally excluded.

> **DM access requires a DM-enabled app password.** App passwords created without "Allow access to your direct messages" checked make every chat call fail with `InvalidToken` ("Bad token method"). App passwords can't be edited, so create a new one with that box ticked and update `BSKY_APP_PASSWORD`.

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env       # then fill in credentials
python server.py
```

Dependencies: `fastmcp` (4.x), `atproto`, `httpx`, `python-dotenv`, `grapheme`, `uvicorn`, plus `tiktoken`, `pandas`, `numpy`, `scipy` and `wordfreq` for the vendored index builder in `comp/`.

## Environment variables (`.env`)

| Variable | Required | Notes |
|---|---|---|
| `BSKY_HANDLE` | yes | e.g. `yourhandle.bsky.social` |
| `BSKY_APP_PASSWORD` | yes | App password from bsky.app/settings/app-passwords |
| `BSKY_SERVICE` | no | PDS/entryway base URL, default `https://bsky.social`. Set for non-Bluesky-hosted accounts |
| `MCP_HOST` | no | Default `0.0.0.0` |
| `MCP_PORT` | no | Default `8765` |
| `MCP_AUTH_TOKEN` | no* | Static bearer token, sent as `Authorization: Bearer <token>` (CLIs, bots, scripts) |
| `MCP_PUBLIC_URL` | for OAuth | Public base URL, e.g. `https://mcp.example.com`; OAuth endpoints and the GitHub callback hang off it |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | for OAuth | GitHub OAuth app; callback URL `<MCP_PUBLIC_URL>/auth/callback` |
| `MCP_ALLOWED_GITHUB_IDS` | for OAuth | Comma-separated numeric GitHub user IDs allowed in (`gh api user --jq .id`) |
| `MCP_JWT_SIGNING_KEY` | no | Signs the OAuth tokens the server issues; defaults to a key derived from the GitHub secret |
| `MCP_RAW_OUTPUT_CHARS` | no | Compact-row output cap, default 72000 |

\* Set the header token, GitHub OAuth, or both. With neither, the server is unauthenticated and logs a warning.

## Deployment (Ubuntu / systemd)

```bash
sudo cp bluesky-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bluesky-mcp
```

Edit `bluesky-mcp.service` to set `User`, `WorkingDirectory`, and `ExecStart` to the actual paths before deploying.

## Architecture

Everything lives in `server.py`. Key sections:

- **Startup**: `atproto.Client` logs in and both caches are populated.
- **Caches**: `feed_cache` and `list_cache` map lowercased human names to AT URIs. On a cache miss, the cache is refreshed before raising an error (handles newly added feeds/lists without restart). Preferences are fetched via raw `httpx` to avoid SDK validation errors on newer preference types.
- **Pagination helpers**: `fetch_by_count` and `fetch_by_time` accept a `fetch_fn` closure and handle all cursor-based pagination. `fetch_by_time` skips items older than the cutoff and stops only when an entire page predates it (ranked feeds interleave old posts among new ones).
- **Where text lives**: `_SERVER_INSTRUCTIONS` (~240 tokens) holds only what is shared across tools — identity, post-ID lifetime, timezone, mutuals, the index → search → thread workflow, account discovery, what a convo is. Clients put it in **every** model call (Claude Code puts it in the system prompt), even when they hide some tools, so it must not grow back into a manual. What one tool does goes in its docstring; what a parameter means goes in its description — docstring `Args:` entries become per-parameter schema descriptions (and are removed from the tool description), and the feed tools share `Annotated` types (`SinceMinutes`, `FeedLimit`, `MinChars`, `QuoteMode`) whose `Field(description=...)` wins over any `Args:` entry for the same parameter. How to read an output goes in a header line of that output: compact feeds open with a `cols:` legend, `get_conversations` with its own.
- **Text-only results**: every tool is registered with `output_schema=None`. Left to infer, FastMCP turns `-> str` into a `{"result": string}` output schema and sends each result twice (text + `structuredContent`); Claude Code passes the model the JSON copy, so the row tables arrived as one escaped line. `tools/list` should show no `outputSchema` on any tool.
- **Errors**: a caller mistake the caller must fix (expired post ID, mutually exclusive params, empty input) raises `ToolError`, so the client gets `isError` rather than a result that reads like data. `_lookup_post_view` is the shared post-ID → PostView path for `get_post_by_id` / `get_post_images`.
- **Feed source resolution**: `get_posts(source=...)` is the general feed tool — `_resolve_source` maps `"timeline"`/`"home"`/`"following"` to the home timeline, then tries the feed cache, then the list cache (refreshing both on miss), and dispatches raw `at://` URIs on their collection segment. `get_author_posts` retrieves the newest authored posts from one account without a search query, with optional replies and reposts. `_fetch_and_render` is the shared scope-fetch + render path.
- **Formatting (compact)**: `get_posts` (in the default `mode="raw"`), `get_author_posts`, `search_posts`, `get_my_likes`, and `get_bookmarks` use `_render_compact_feed`, which emits a compact pipe-delimited schema: `id | author | time | metrics | refs | text`. It renders **every** eligible post in scope — `max_posts` (newest N in feed order) is the only thinning; to read a window too large for rows, use `mode="index"` rather than a sample of it. The one exception is the output size cap `_RAW_OUTPUT_CHAR_BUDGET` (72,000 chars ≈ 15–20k tokens, env `MCP_RAW_OUTPUT_CHARS`), under Claude Code's default 25k-token tool-result limit: rows past it are cut newest-kept and a footer reports `N more posts not shown` plus a caller-specific `overflow_hint`. Quote entries are charged to the first kept row that references them, so kept quotes are always Q1..Qk. Quoted posts are deduplicated into a top-of-output `quotes:` table referenced by `Q1`/`Q2`/...; times are in America/Toronto local format (`HHMM` for windows <24 h, `DD HHMM` for bounded windows under 28 days, full `YY-MM-DD HHMM` for unbounded/28-day+ fetches and quote-table entries). Image posts with <30 chars of text get their alt text inlined in the `[img: ...]` marker. Each post gets a numeric ID written to a SQLite store (`post_ids.db`, 1-hour TTL) so `get_post_by_id` can return full detail later. Pipe and newline characters in text are replaced with `¦` and a space so they don't break the row format. Callers can pass `extra_cols` (URI → string) to insert a column after the time field.
- **List management**: list writes use repository records directly. Only lists owned by the authenticated DID may be changed. Batch member changes resolve handles to DIDs, skip duplicates/existing state, and use `applyWrites` in atomic chunks; list deletion removes every membership record before deleting the list record. Renames update the local name cache immediately.
- **Actor discovery**: `search_actors` wraps both `searchActors` and query-less `getSuggestions`; `get_known_followers` returns profiles followed by the viewer that also follow the subject. Profile rows retain DIDs and viewer relationship flags for follow-up tool calls.
- **Draft updates**: `update_draft` preflights the ID because upstream silently ignores missing drafts, preserves embeds/media positionally by default, and re-reads the draft to verify the write. Changing thread length requires explicitly disabling embed preservation.
- **Formatting (verbose)**: `get_thread`, `get_notifications`, `get_drafts`, and `get_post_by_id` use the verbose `format_post_text` per-post block joined with `---`. Posts with ≥100 likes get a `[!]` viral flag. `get_post_by_id` additionally appends image CDN URLs (`URLs: ...`) and points image posts to `get_post_images`, which downloads API-supplied CDN renditions and returns actual MCP image content blocks. It handles legacy image embeds (4 max), galleries (10-item authoring soft limit / 20-item schema ceiling), and record-with-media; quoted-post media is not included.
- **Bookmarks**: `get_bookmarks` is paginated (`limit` = page size, `page` 1-based). It enumerates the full bookmark list (capped at 5,000) to report the true total and `page n/N`, shows the bookmark date as a `bm:YY-MM-DD` column next to the post creation time, and lists bookmarks whose posts are deleted/unavailable with their AT URIs so they can be pruned via `delete_bookmarks(uris=...)`.
- **Rich text facets**: `create_post` and `create_thread` run `_detect_facets` over the text — URLs, `@mentions` (resolved to DIDs via `resolveHandle`; unresolvable ones stay plain text), and `#hashtags` become clickable facets with UTF-8 byte offsets. Post text is validated against both lexicon limits (300 graphemes and 3,000 UTF-8 bytes). Keep `_test_facets.py` in sync with the regexes.
- **Search**: `search_posts` calls **`app.bsky.feed.searchPostsV2`** via raw XRPC (`_xrpc_get`) — the endpoint is not in the `atproto` SDK (0.0.68), so raw JSON posts are hydrated into `AppBskyFeedDefs.PostView` models with `get_or_create(..., strict=False)` before hitting the shared renderers. `query` is optional: the lexicon accepts a query, at least one filter, or both, so `following=True` alone browses posts from followed accounts. Exposed filters: `following`, `authors`, `exclude_authors`, `mentions`, `hashtags`, `exclude_hashtags`, `domains`, `urls`, `languages`, `has_media`, `has_video`, `exclude_replies`/`replies_only`, `thread_root_uri`, `since`, `until`, `all_time`. List params accept a list or a comma-separated string (`_v2_list`); booleans are sent as `"true"`/`"false"` since httpx renders Python bools as `"True"`. v2 renamed the chronological sort **`latest` → `recent`**; `_V2_SORTS` maps `latest` → `recent` for direct Python callers, but the tool's `Literal["recent", "top"]` means MCP clients get a validation error for `latest` (intended — the schema is re-sent each session). The old singular `author`/`lang` survive as aliases folded into `authors`/`languages` by `_ClientArgShim` (see below), as does `since_minutes` → `since_hours` (which is therefore a float). The server pre-validates the mutually exclusive `exclude_replies`/`replies_only` pair and the query-or-filter rule (both are 400s otherwise), and surfaces XRPC error bodies via `_xrpc_error_message`. `_run_v2_search` handles pagination (until `limit` posts survive `min_chars`) and returns `raw_seen` alongside `kept`, which separates "the search matched nothing" from "`min_chars` dropped everything".
- **Search time bounds and the recent-index trap**: v2 searches only a recent window (measured at roughly the last month) unless `all_time=True` — outside it the API returns an **empty result, not an error**, which reads as "nobody posted about this". `_search_posts_impl` therefore retries once with `allTime=true` whenever a search matched nothing (`raw_seen == 0`) and reports `auto-expanded past the recent index` in the header. The retry is deliberately skipped when `raw_seen > 0`, since the search did match and `min_chars` was the filter. Bounds come from either `since_hours` (relative) or `since`/`until` (absolute, and filtering on **indexing** time, not creation time) — the two are mutually exclusive. `hitsTotal` saturates at 10000, so it renders as `10000+ hits` rather than `~10000`.
- **Search rendering**: `search_posts` passes `quote` through to `_render_compact_feed` like the feed tools, plus a `window_minutes` derived by `_window_minutes_from_bounds` from whichever time bound was given. That window is what lets the renderer compact timestamps (`HHMM` instead of `YY-MM-DD HHMM`), so bounded searches are meaningfully cheaper per row than unbounded ones.
- **Timeline index (`get_posts(mode="index")`)**: instead of sampling rows down to a token budget, the whole scoped window is compressed into **keyqueries** — term conjunctions that simultaneously *name* a discussion and *are* the Bluesky search query that retrieves it. 3,047 posts render in 522 tokens (274× cheaper than compact rows) at ~96% recall when the entries are re-issued live. Nothing is dropped: detail is recovered on demand via `search_posts` / `get_thread`. `_render_index` fetches through the same `_fetch_scope` as raw mode (forcing `min_chars=0` — the index's own gates handle noise, and short replies carry the thread structure it merges on), converts the feed items with `_postviews_to_frame`, then runs `comp.pipeline.prepare(frame=...)` → `run(budget=...)` → `comp.encode.encode`. `budget` (default 400) is a dial, not an outcome: entries are emitted in decreasing value order, so a bigger budget only adds topics. The header's expansion hint is source-aware — `following=1` for the home timeline, and for a feed/list a note that search is global (nothing in search reproduces "this feed"). Timing: ~2 s per 1,000 posts (0.7 s @500, 1.7 s @1k, 7 s @3k), which is why `_heartbeat` matters.
- **The vendored `comp/` package**: a snapshot of the separate `compressor` research repo (`src/comp/`), which stays the lab — **fixes must be ported both ways**. Four deltas from upstream, all upstream-compatible: `data.from_frame(df)` splits the thread-building half out of `data.load(path)`; `pipeline.prepare(frame=...)` takes a preloaded frame; `bg.BG_DIR` resolves `data/bg/` against the package rather than the CWD; `encode.encode` gained `mutuals=` / `scope=` / `source_note=`. `data/bg/firehose_idx.pkl` (29 MB, term → doc ids over a 247k-post firehose sample — the reference search engine that answers "how specific is this conjunction") and `subject.pkl` (12 MB, per-term headline log-odds and proper-noun rate) are loaded lazily by `_comp_artifacts()` on the first index call, behind a lock, so a server that never indexes never pays the ~200 MB. **Both were built from a single day of firehose (2026-08-07) and the gate thresholds were calibrated on one timeline** — expect drift, and rebuild via the lab's `scratch/build_background.py` if index quality degrades.
- **`_postviews_to_frame` column contract**: the whole adapter surface between `atproto` and `comp/`. `text`/`ext_title`/`ext_desc`/`alt_text`/`tags` are `comp.data.SEARCHABLE` — they are what Bluesky's own search indexes, so omitting any of them yields entries the search cannot resolve (a post reading only "CentCom's nightmare." matches `Pakistan` solely through its link card). `reply_root` builds threads, `quote_uri` and `ext_uri` supply the quote and shared-article merge edges. `_get_external_meta` (title + description, extending `_get_external_url`) and `_record_tags` (record tags + `#tag` facets) exist for this.
- **Mutuals**: a hand-curated roster in its own `mutuals.db` — deliberately *not* a table in `post_ids.db`, which is 1-hour TTL scratch that is safe to delete. Keyed on DID so a handle change updates in place; `_mutuals_present_handles` matches on DID with a handle fallback. `_render_compact_feed` and `_render_index` both emit `mutuals here: …` naming only roster members present in the fetched window (omitted entirely when empty). In index mode it is the only author signal at all, since entries are topics; in raw mode it marks which of the already-visible handles are the user's people. Manual only (`add_mutuals`/`list_mutuals`/`remove_mutuals`) — no follow-graph queries.
- **Link cards**: `create_post` and `create_thread` accept an `embed_url` / `[embed: url]` marker; `_fetch_link_card` fetches OG metadata to build the card.
- **Thread posts**: `create_thread` splits on `---` separators and chains posts as replies to each other.
- **Auth (`_build_auth`)**: the endpoint is always `/mcp`; no secret goes in the URL, because uvicorn and proxies log request paths. Two sources, either or both: `_HeaderTokenVerifier` checks `Authorization: Bearer` against `MCP_AUTH_TOKEN` with `hmac.compare_digest` (CLIs, bots, scripts), and `_AllowlistedGitHubProvider` runs FastMCP's GitHub OAuth proxy for connector clients that can't send headers (claude.ai, Claude apps). Both together go through `MultiAuth`. GitHubProvider alone admits *any* GitHub user, so the subclass re-checks the GitHub numeric ID against `MCP_ALLOWED_GITHUB_IDS` in `load_access_token` on every request. The OAuth flow requests **no GitHub scopes**: identity is all the server needs, and `MultiAuth` applies one required-scope set to every source, so an empty set is what lets the header token pass. Client registration (DCR) only accepts redirect URIs matching `_OAUTH_REDIRECT_PATTERNS` (claude.ai/claude.com and loopback); add ChatGPT's callback there if it is ever needed. FastMCP keeps OAuth client registrations and tokens in an encrypted file store under `~/.local/share/fastmcp/oauth-proxy/` for the service user, so they survive restarts. Its encryption key derives from `MCP_JWT_SIGNING_KEY`; changing that key logs every OAuth client out.
- **Session refresh**: `_ensure_session()` is called before raw XRPC calls — it decodes the access JWT's `exp` locally and skips the network entirely while the token is fresh; the `atproto` SDK handles session refresh automatically for SDK calls.
- **SSRF guard**: link-card fetches (`_get_validating_redirects`) follow redirects manually so every hop is checked against private/loopback/link-local addresses, not just the first URL.
- **Direct messages (chat)**: DMs live behind the central chat service (not the account's PDS), reached via `_dm()` → `client.with_bsky_chat_proxy().chat.bsky.convo`, which injects the `atproto-proxy: did:web:api.bsky.chat#bsky_chat` header on a session-fresh client. A **group chat is just a convo with `kind="group"`**, so `get_conversations`/`get_messages`/`send_message`/`mark_read` cover 1:1 and group identically (group management is not exposed). `messageView.sender` carries only a DID, so renderers map DID→handle from the convo's `members` (`_member_map`). `get_messages` defaults to `unread_only=True`: it reads `ConvoView.unread_count` (the only read-state signal — there's no per-message read flag) and, since messages come back newest-first, treats the newest N as unread, plus a small `context` window of preceding messages; unread rows are tagged `[unread]` under a divider. `_format_dm_reply_context` renders the API-supplied parent message one level deep, and `_format_dm_shared_post` renders shared-record author/text plus one level of post embed; missing, blocked, and detached records get explicit markers. DM text is validated against the chat lexicon's 1000-grapheme / 10000-byte limits (`_validate_message_text`) and reuses `_detect_facets` for rich text. `send_message` remains text-only and takes either an existing `convo_id` or a `to_handle` (resolved via `_resolve_did` → `get_convo_for_members` to open/find a 1:1).

## MCP tools exposed

`list_sources`, `get_list`, `create_list`, `update_list`, `add_list_members`, `remove_list_members`, `delete_list`, `get_posts`, `get_author_posts`, `get_post_by_id`, `get_post_images`, `get_thread` (with `full=True` for whole-thread-from-root), `get_notifications`, `get_profile`, `search_actors`, `get_known_followers`, `search_posts`, `create_post`, `create_thread`, `get_bookmarks`, `delete_bookmarks`, `get_drafts`, `create_draft`, `update_draft`, `delete_draft`, `get_my_likes`, `add_mutuals`, `list_mutuals`, `remove_mutuals`, `get_conversations`, `get_messages`, `send_message`, `mark_read`

Some MCP clients send `wait_for_previous` (a sequencing hint) on every call. No tool declares it: the `_ClientArgShim` middleware strips it before validation, and also folds hidden aliases into canonical parameters per `_ARG_ALIASES` — `author`/`lang`/`since_minutes` on `search_posts`, `since_hours` on the minute-scoped feed tools (`get_posts`, `get_my_likes`, `get_notifications`, `get_messages`). Passing both an alias and its canonical name is a `ToolError`.

> **Why middleware, not `Depends()` / `CallArgument()`.** FastMCP 4 removed `exclude_args` and rejects undeclared arguments (`additionalProperties: false`, "Unexpected keyword argument"). Its suggested replacement, `Depends()`, resolves server-side and *filters out* any client-supplied value for a dependency parameter — `CallArgument()` included — so neither can carry a value the client sends. Rewriting `context.message.arguments` in an `on_call_tool` middleware runs before validation and was verified in-process and over HTTP. `requirements.txt` pins `fastmcp>=4.1,<5`; re-check the shim after any major upgrade.

## Connecting clients

**Claude Code / CLIs (header token)** — add to `~/.Codex/mcp.json` (or `claude mcp add --transport http bluesky https://YOUR_SERVER/mcp --header "Authorization: Bearer $TOKEN"`):

```json
{
  "mcpServers": {
    "bluesky": {
      "type": "http",
      "url": "https://YOUR_SERVER/mcp",
      "headers": { "Authorization": "Bearer YOUR_MCP_AUTH_TOKEN" }
    }
  }
}
```

**Codex** — in `~/.codex/config.toml`, with the token in an environment variable:

```toml
[mcp_servers.bluesky]
url = "https://YOUR_SERVER/mcp"
bearer_token_env_var = "BSKY_MCP_TOKEN"
```

**claude.ai / Claude apps (OAuth)** — add a custom connector with URL `https://YOUR_SERVER/mcp` and no credentials. Claude discovers the OAuth endpoints, you approve the consent screen, then sign in with an allowlisted GitHub account.
