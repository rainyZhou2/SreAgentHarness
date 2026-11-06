# SRE Harness desktop client

English | [中文](README.zh-CN.md)

A native desktop client for the SRE harness, built with [Wails v2](https://wails.io): Go plus the operating system's WebView, so there is no bundled browser (~9 MB on macOS). It is a separate Go module and talks to the server only through the public `/v1` API, using the same authentication and roles as any other client.

## What it does

It is a chat client in the style of Codex / Claude desktop, built on the harness's sessions and turns.

- **Chats** (sidebar): your conversations, most recently used first. A dot marks chats where the agent is working. **New chat** starts one from your first message, and the suggestions help you start. The search box filters chats by title, and **Archived** switches to archived chats. Each chat's ⋯ menu (also in the chat header) can **rename**, **archive**/unarchive, **export as Markdown** (question, tool calls and answer per turn) and **delete** it. Click the title in the header to rename the chat.
- **Replies**: each message you send becomes a turn; the agent answers it with a run. A reply shows:
  - a **work log**, open while working and collapsed to "Worked for 9s · 2 tool calls" when done:
    - memory search;
    - reasoning summaries, with streamed model text while it thinks;
    - tool calls (name, arguments, status) that you can click open to see the output;
  - an **answer** (light Markdown) and **hypotheses** with status and confidence;
  - run id, model calls and tokens.
- **Approvals inline**: when a tool needs approval, its card shows the risk class, target, expiry and action hash, with **Approve** and **Reject** (with an optional reason).
- **Approvals inbox**: the sidebar's **Approvals** lists pending approvals from every chat in the tenant, with a count badge. You can approve or reject them there, or open the chat. The app checks every few seconds and posts a macOS notification for each new approval; clicking the notification opens the inbox. macOS asks once for notification permission.
- **Composer**: Enter sends and Shift+Enter adds a new line. Enter that confirms an IME candidate (Chinese input) does not send. While the agent works:
  - new messages are queued and run in order;
  - ■ stops the current reply;
  - **Pause** and **Resume** are available under the reply.

  Typing `/` opens a picker: skills (`/<skill>` turns a manual skill on for this chat) and tools (inserts `use <tool> {…}` with the tool's arguments filled in from its schema). ↑/↓ moves, Enter or Tab picks, and Esc closes the picker.
- **Customize**: the tenant's runtime extensions, in five tabs.
  - **Model**: the tenant's model. Use Anthropic (model and effort) or an OpenAI-compatible endpoint (base URL and model). **Test** sends a short request before saving. The top of the tab shows the model that new runs use. With a model set, the agent chooses tools itself.
  - **MCP servers**: add a server by URL and headers, then *Connect & list tools*. Tick the tools the agent may call and give each a risk class (R0–R3) and an optional target argument.
  - **HTTP tools**: endpoint, method, risk and input schema.
  - **Skills**: Markdown playbooks added to the system prompt, either *always* or *manual* (only in chats that mention `/<name>`).
  - **Tools**: everything a run can be offered, with its source, risk and whether the default profile allows it.

  Reading needs the viewer role, changes need admin. Header values and API keys are write-only: leave them blank to keep the saved value. In a chat, `use <tool> {json}` asks the agent to call a tool.
- **Plugins**: the server's plugin inventory (needs the admin role).

The server enforces every permission. Commands carry the run's current `expected_version`, so a stale view gets `409` rather than acting on old state.

## Build and run

Requirements: Go 1.26+.
- macOS: Xcode command line tools.
- Linux: `libgtk-3-dev` and `libwebkit2gtk-4.1-dev`.
- Windows: WebView2.

No Node.js or Wails CLI is needed.

```bash
./build.sh        # macOS: build/bin/SRE Harness.app
./build.sh run    # build and open
```

On first launch, open **Connection** and set:
- the server URL (default `http://127.0.0.1:8080`);
- the authentication mode:
  - **Development headers**: tenant, actor, roles. Only works against a server in `dev` auth mode.
  - **Bearer token**: a static token or an OIDC JWT.

Settings are saved to `<user config dir>/sre-harness-desktop/settings.json` (mode 0600). The bearer token is kept in memory only.

## Browser preview

The same binary can serve the UI to a browser instead of opening a window. This is useful for UI work and automated checks. It binds to loopback only.

```bash
SRE_DESKTOP_PREVIEW=127.0.0.1:34115 "build/bin/SRE Harness.app/Contents/MacOS/sre-harness-desktop"
```

Then open http://127.0.0.1:34115/.

## Layout

| Path | Purpose |
|---|---|
| `main.go` | Wails window and options; switches to preview mode when `SRE_DESKTOP_PREVIEW` is set |
| `app.go` | methods bound to the UI (`window.go.main.App.*`): sessions, transcript, send, run view, commands, live watch; all HTTP happens in Go |
| `inbox.go` | chat search, rename, archive, delete and Markdown export; the approvals inbox and system notifications |
| `harness/` | dependency-free `/v1` client: auth headers, idempotency keys, errors, SSE parsing with resume |
| `preview.go` | browser preview bridge (`/__call/<Method>`, `/__events`) |
| `frontend/dist/` | the UI: plain HTML/CSS/JS, no build step; it renders server data as text only |

## Tests

```bash
go test ./...
```
