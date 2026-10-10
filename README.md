# mcp-bsky

A Model Context Protocol (MCP) server for Bluesky. It exposes a Bluesky account to MCP clients with tools for reading feeds and threads, searching Bluesky, managing lists and drafts, creating posts, reading bookmarks and likes, and working with direct messages.

The server uses the official AT Protocol/Bluesky APIs through `atproto` plus a few raw XRPC calls for APIs not yet represented by the SDK.

## Features

- Read the home timeline, custom feeds, lists, author feeds, threads, notifications, bookmarks, likes, and profiles.
- Search posts with Bluesky's `searchPostsV2` filters and discover accounts.
- Create posts and multi-post threads with links, mentions, hashtags, replies, quotes, and link cards.
- Manage Bluesky lists and saved drafts.
- Read and send direct messages, including existing group conversations.
- Fetch image bytes from image posts for MCP clients that support image content.
- Optional compact/index rendering for large timeline windows to make research over thousands of posts practical for LLM clients.
- Streamable HTTP MCP transport via FastMCP.

## Requirements

- Python 3.11+ (3.12 recommended)
- A Bluesky account
- A Bluesky app password

Create an app password in Bluesky under **Settings → Privacy and security → App passwords**. Do not use your main Bluesky password.

If you want to use the direct-message tools, create the app password with **Allow access to your direct messages** enabled. App passwords without that permission can still use the non-DM tools.

## Installation

Clone the repository and create a virtual environment:

```bash
git clone https://github.com/aaricie/mcp-bsky.git
cd mcp-bsky

python -m venv .venv
```

Activate it:

```bash
# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Copy the environment template:

```bash
# Linux/macOS
cp .env.example .env

# Windows PowerShell
Copy-Item .env.example .env
```

Then edit `.env` and set at least:

```dotenv
BSKY_HANDLE=yourhandle.bsky.social
BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
```

Start the server:

```bash
python server.py
```

By default the MCP endpoint is:

```text
http://localhost:8765/mcp
```

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `BSKY_HANDLE` | yes | — | Bluesky handle for the account the MCP server will use. |
| `BSKY_APP_PASSWORD` | yes | — | Bluesky app password. Use a DM-enabled app password if you need DM tools. |
| `BSKY_SERVICE` | no | `https://bsky.social` | PDS/entryway base URL. Change this for accounts hosted on another PDS. |
| `MCP_HOST` | no | `0.0.0.0` | Interface on which the HTTP server listens. Use `127.0.0.1` if the server should only be reachable locally. |
| `MCP_PORT` | no | `8765` | HTTP port. |
| `MCP_AUTH_TOKEN` | no | empty | Static bearer token. Clients send it as `Authorization: Bearer <token>`. Use a long random value. |
| `MCP_PUBLIC_URL` | for OAuth | — | Public base URL of the server, e.g. `https://mcp.example.com`. |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | for OAuth | — | Credentials of a GitHub OAuth app whose callback URL is `<MCP_PUBLIC_URL>/auth/callback`. |
| `MCP_ALLOWED_GITHUB_IDS` | for OAuth | — | Comma-separated numeric GitHub user IDs allowed to sign in (`gh api user --jq .id`). |
| `MCP_JWT_SIGNING_KEY` | no | derived | Signs the OAuth tokens the server issues. Set it so rotating the GitHub secret doesn't sign every client out. |
| `MCP_RAW_OUTPUT_CHARS` | no | `72000` | Size cap for row-per-post output; posts past it are reported as not shown. |

`.env` is ignored by Git. Never commit your app password, MCP auth token, OAuth secrets, session tokens, SSH credentials, or reverse-proxy credentials.

## Authentication

The MCP endpoint is always `/mcp`; no credential is ever part of the URL. Configure one or both of:

- **Bearer token** (`MCP_AUTH_TOKEN`) for clients that can send headers: Claude Code, Codex, scripts, bots.
- **GitHub OAuth** (`MCP_PUBLIC_URL`, `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET`, `MCP_ALLOWED_GITHUB_IDS`) for connector clients that can't send headers, including ChatGPT and Claude apps. The server runs the OAuth flow itself, shows a consent screen, and admits only the GitHub accounts you list. It requests no GitHub scopes, so it learns who you are and nothing else.

With neither configured, the server is unauthenticated and logs a warning at startup. That is only appropriate when it is bound to `127.0.0.1`.

## Connecting an MCP client

Without authentication (local only), point a streamable-HTTP MCP client at `http://127.0.0.1:8765/mcp`.

**Claude Code** (bearer token):

```bash
claude mcp add --transport http bluesky https://mcp.example.com/mcp --header "Authorization: Bearer YOUR_MCP_AUTH_TOKEN"
```

or in an MCP JSON config:

```json
{
  "mcpServers": {
    "bluesky": {
      "type": "http",
      "url": "https://mcp.example.com/mcp",
      "headers": { "Authorization": "Bearer YOUR_MCP_AUTH_TOKEN" }
    }
  }
}
```

**Codex** (`~/.codex/config.toml`, token read from an environment variable):

```toml
[mcp_servers.bluesky]
url = "https://mcp.example.com/mcp"
bearer_token_env_var = "BSKY_MCP_TOKEN"
```

**claude.ai / Claude apps** (OAuth): add a custom connector with the URL `https://mcp.example.com/mcp` and no credentials, approve the consent screen, then sign in with an allowed GitHub account.

**ChatGPT** (OAuth): add a custom MCP server with the Server URL `https://mcp.example.com/mcp` and select OAuth. Leave the discovered authorization, token, registration, authorization-server, and resource URLs unchanged. ChatGPT registers its OAuth client with this server; its callback is either `https://chatgpt.com/connector/oauth/{callback_id}` or `https://chatgpt.com/connector_platform_oauth_redirect`. Both are allowed by the server. Copy the exact callback shown in ChatGPT's MCP management page if you need to inspect a failed registration. The GitHub OAuth app's callback remains `https://mcp.example.com/auth/callback`: it is a separate hop from the ChatGPT callback. Restart the deployed MCP service after changing the OAuth redirect allowlist.

Client configuration formats differ, so consult the documentation for your MCP client if it uses different field names.

## MCP tools

### Feeds, posts, and threads

| Tool | Purpose |
|---|---|
| `list_sources` | List available timeline/feed/list sources that can be passed to `get_posts`. |
| `get_posts` | Read the home timeline, a saved feed, a list feed, or a raw AT URI source. Supports time/count scoping and raw or compressed index rendering. |
| `get_author_posts` | Read recent posts from one account, with controls for replies and reposts. |
| `get_post_by_id` | Expand a numeric post ID returned by compact feed output into full post detail. |
| `get_post_images` | Download images attached to a post and return them as MCP image content. |
| `get_thread` | Fetch a post thread, including parents/replies; `full=True` resolves and fetches from the root. |
| `get_notifications` | Read recent account notifications with optional reason/time filters. |
| `get_my_likes` | Read posts liked by the authenticated account. |

### Search and account discovery

| Tool | Purpose |
|---|---|
| `search_posts` | Search Bluesky posts with text and structured filters such as authors, hashtags, domains, languages, media, replies, and time bounds. |
| `get_profile` | Look up a Bluesky profile by handle or DID. |
| `search_actors` | Search accounts, or retrieve account suggestions when no query is supplied. |
| `get_known_followers` | Find people followed by the authenticated account who also follow another account. |

### List management

| Tool | Purpose |
|---|---|
| `get_list` | Read a list and its members. |
| `create_list` | Create a Bluesky list. |
| `update_list` | Rename or edit a list owned by the authenticated account. |
| `add_list_members` | Add one or more accounts to a list. |
| `remove_list_members` | Remove one or more accounts from a list. |
| `delete_list` | Delete a list and its membership records. |

### Posting

| Tool | Purpose |
|---|---|
| `create_post` | Create a post or reply. Detects URL, mention, and hashtag facets and can attach a link card/quote where supported. |
| `create_thread` | Create a multi-post thread from text separated with `---`. |

### Bookmarks

| Tool | Purpose |
|---|---|
| `get_bookmarks` | Read saved bookmarks with pagination and compact post IDs. |
| `delete_bookmarks` | Remove bookmarks by recent numeric post ID, AT URI, or bsky.app URL. |

### Drafts

| Tool | Purpose |
|---|---|
| `get_drafts` | Read saved Bluesky drafts. |
| `create_draft` | Save a single-post or multi-post draft without publishing it. |
| `update_draft` | Replace the contents of an existing draft, optionally preserving embeds/media. |
| `delete_draft` | Delete a saved draft. |

### Mutual roster

These tools manage a small local roster used only as extra context in feed/index output. The roster is stored in the local `mutuals.db` SQLite database and is not derived automatically from the Bluesky follow graph.

| Tool | Purpose |
|---|---|
| `add_mutuals` | Add handles/DIDs to the local mutual roster. |
| `list_mutuals` | Show the current local mutual roster. |
| `remove_mutuals` | Remove entries from the local mutual roster. |

### Direct messages

DM tools require a Bluesky app password created with **Allow access to your direct messages** enabled.

| Tool | Purpose |
|---|---|
| `get_conversations` | List accepted conversations or message requests, including direct and group conversations. |
| `get_messages` | Read messages in a conversation, optionally focusing on unread messages plus context. |
| `send_message` | Send a message to an existing conversation or start/reuse a 1:1 conversation by handle. |
| `mark_read` | Mark a conversation read, optionally through a specific message. |

Group-chat membership/admin operations are intentionally not exposed.

## Large-window index mode

`get_posts(mode="index")` is intended for research workflows where an LLM needs to understand a large timeline window without receiving thousands of full post rows.

Instead of sampling posts, the server builds compact topic/key-query entries over the fetched scope. Those entries can then be fed back into `search_posts` to recover the underlying discussion in detail. The index implementation lives in the vendored `comp/` package and uses the bundled `data/bg/` background artifacts.

The background artifacts are intentionally lazy-loaded, so ordinary MCP use does not pay their memory cost unless index mode is requested.

## Local state

The server creates local SQLite databases at runtime:

- `post_ids.db` stores short-lived numeric post-ID mappings used by `get_post_by_id` and related tools.
- `mutuals.db` stores the optional manually managed mutual roster.

These database files and their WAL/SHM companions are ignored by Git.

## Running as a service

A generic `bluesky-mcp.service` systemd unit is included. Edit its `User`, `WorkingDirectory`, `EnvironmentFile`, and `ExecStart` paths before installing it.

```bash
sudo cp bluesky-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bluesky-mcp
```

For a server exposed beyond localhost, put it behind a TLS reverse proxy (OAuth requires HTTPS) and configure authentication as described above. FastMCP stores OAuth client registrations and tokens in an encrypted file store under the service user's data directory (`~/.local/share/fastmcp/oauth-proxy/` on Linux), so they survive restarts.

## Security notes

This server acts with the permissions of the configured Bluesky app password. Several tools can publish posts, modify lists/drafts/bookmarks, or send direct messages. Only connect MCP clients you trust.

For local-only use, bind `MCP_HOST=127.0.0.1`. If you expose the service to a LAN or the Internet, use HTTPS through a reverse proxy, configure authentication, restrict network access where possible, and keep `.env` outside source control.

Link-card fetching includes a redirect-aware SSRF guard that rejects private, loopback, and link-local destinations.

## Development

The main server is intentionally kept in `server.py`. The `_test_*.py` scripts are self-contained regression checks for selected rendering/parsing behavior and do not require deployment credentials.

Internal architecture notes for coding agents are in `AGENTS.md` and `CLAUDE.md`.

## License

MIT — see [LICENSE](LICENSE).
