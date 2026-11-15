# SRE Harness desktop frontend (WP0–WP1)

React + TypeScript frontend for the desktop client, built as the WP0 vertical
slice plus WP1 session management and approval inbox described in [`docs/AGENT_FIRST_DESKTOP_DESIGN.md`](../../docs/AGENT_FIRST_DESKTOP_DESIGN.md)
(§2 information architecture, §3 architecture, §4.1 projections, §6 frontend
engineering).

The production build goes to `dist-next/`; the legacy `dist/` bundle is owned by
the previous UI and is never written by these scripts.

## Visual language

The React shell keeps the existing desktop client language: monochrome surfaces,
one blue accent, 7/10/12 px control/panel/shell radii, the same 24 px outline icon
set and the logo mark. The sidebar, composer and overlays use the same translucent
glass material over an ambient canvas; reading surfaces stay opaque.

## Commands

```bash
npm install
npm run typecheck   # tsc --noEmit
npm test            # Vitest + Testing Library
npm run build       # typecheck + Vite production build -> dist-next/
```

The package manager is npm. Node 20+ and npm 10+ are expected.

## Structure

```text
src/
├── app/              App bootstrap, providers, ConnectedApp, AppShell
├── bridge/           Typed Wails bridge facade, Zod validation, error model
├── components/ui/    Reusable primitives (badges, errors, panels, dialog, markdown)
├── features/
│   ├── agent-run/    RunProjection view and run pause/resume/stop controls
│   ├── blocks/       Projection block renderers (status … action … unknown)
│   ├── composer/     Message composer (IME-safe Enter/Shift+Enter)
│   ├── connect/      Boot flow, local/remote connection screen, connection context
│   ├── conversation/ Home, conversation header and transcript viewport
│   ├── drawer/       Right-hand detail drawer with focus trap
│   ├── sessions/     Sidebar: server-side search/filter, rename/archive/delete/export
│   ├── approvals/    Global approval inbox with server-authoritative decisions
│   ├── works/        Works inbox, goal/stage view and safe command controls
│   └── settings/     Language, theme and connection details
├── hooks/            Query/event/watch hooks, theme, focus trap, connection health
├── i18n/             en/zh dictionaries and provider (system default + switch)
├── lib/              Query keys/client, formatting, status mappings, version checks
├── state/            Zustand UI state and streaming buffers (non-authoritative)
├── styles/           Design tokens and global base styles
└── test/             Vitest setup, mock bridge, fixtures and component tests
```

All server data flows through `window.go.main.App.*`; the UI never talks to the
network and does not import `wailsjs`. Bridge payloads are validated with Zod,
and the projection parser degrades unknown block kinds to read-only cards and
missing required fields to per-block error cards.

## Behavior notes

- **Connection**: `GetSettings` → local mode starts the bundled runtime with
  `ConnectLocal`; remote mode shows the connection screen (server URL, bearer
  token, optional development headers, or SSO). SSO uses the server’s
  `/api/auth/config` when issuer/client ID are not filled in; the Go side runs
  Authorization Code + PKCE in the system browser, stores the refresh token in
  the OS keychain and restores the session on launch. Access tokens are refreshed
  before expiry; after 15 minutes without activity the SSO session locks and the
  lock screen clears the in-memory token. Identity and sessions load after connect.
- **Streaming**: runtime events (`run:event`, `run:delta`, `run:stream-error`,
  `approvals:update`) invalidate projections, batch token deltas (~60 ms) and
  surface interrupted streams with a reconnect action. A 3 s projection poll is
  the fallback when the stream is unavailable.
- **Composer**: the goal bar above the input shows the current session title,
  latest-run status and jump-to-latest/rename/delete actions. The input card has
  a `+` menu (skills/tools, context), a server-role chip, a read-only current
  model chip, a circular send button and a metrics line (turns, tool steps,
  tokens and tokens/s when the latest run exposes them). `/` opens a skills/tools
  menu; picking a skill inserts `/name`, picking a tool inserts `use name {}`. The
  permission chip opens the effective role/tool-profile summary; the model chip
  opens the current model and tenant model routing.
  A context panel pins environment, comma-separated services and a local time
  window; the turn is sent through `SendTurn` with those fields.
- **Sessions**: server-side `SearchSessions` (active/archived + title query); row
  actions rename, archive/unarchive, delete and Markdown export; the open session
  closes when it is archived or deleted.
- **Works**: the sidebar opens a Works inbox (active/closed/all), with goal,
  subjects, stages, waiting reason, actions, last outcome and the safe controls
  (`pause`, `resume`, `wake`, `reopen`) from the server view. Commands send the
  Work version; destructive/replan controls are intentionally not exposed yet.
- **Network**: Settings stores an HTTP(S)/SOCKS proxy and an optional PEM CA
  bundle. They are applied to the team HTTP/SSE client and OIDC discovery; a
  diagnostics action checks `/healthz` through the configured transport. PAC is
  not supported yet.
- **Governance**: admins can open a read-only Governance dialog from Settings
  with audit entries, role bindings and revocation events. The server enforces
  the admin role; the UI only hides the entry point.
- **Approvals**: the global inbox lists pending approvals and sends
  `ApproveFromInbox` with the exact run, approval and intent hash. The badge is
  refreshed by `approvals:update` and an 8 s poll. No optimistic decision.
- **Commands**: run pause/resume/stop and action approve/reject send
  `expected_version` from the projection's `run_version` (0 when absent). Action
  decisions use `{approval_request_id, action_hash}` where `action_hash` falls
  back to the block's `intent_hash`. No optimistic success; conflicts and
  unknown effects are shown explicitly and never retried automatically.
- **Intermediate states**: loading/empty/error/offline/forbidden/stale/partial are
  covered for sessions, transcript, runs, cards and operations, with retry or
  another explicit next step on errors.
- **Accessibility**: keyboard navigation, dialog/drawer focus trap and Escape
  close with focus restoration, text-based statuses (no color-only meaning),
  restrained `aria-live` (run status only; token deltas are not announced).

## Tests

`src/test/` holds the Vitest setup, the in-memory bridge mock and fixtures. It is
only imported by tests, so no fixtures reach the production bundle.

- `BlockRenderer.test.tsx`: unknown block kind degrades read-only (raw payload is
  never rendered, no executable controls); an action missing required fields
  becomes an invalid read-only block with no approve/reject buttons.
- `ActionBlock.test.tsx`: no success state until the server confirms the command
  (asserts the exact command payload and `expected_version`), and a 409 conflict
  stays explicit without a success state.
- `ApprovalInbox.test.tsx`: decision sends the exact run/approval/intent hash and
  the empty state is explicit.
- `SessionMenu.test.tsx`: rename goes through the bridge with the new title.
- `WorksInbox.test.tsx`: a safe Work command sends the Work version and command
  type through the bridge.
- `Composer.test.tsx`: slash insertion and the pinned context payload.
- `GovernanceDialog.test.tsx`: audit, role and revocation tables render parsed
  server records.

## Known limitations (WP0–WP1)

- No skills/memory, model or tool settings pages yet; those are WP2+ and the
  shell is ready for them. Works and the approval inbox are implemented.
- The model and permission chips expose server truth; changing tenant model
  routing or policy remains an admin action in Settings/Web.
- Metrics come from the latest run projection; cache-hit percentage is not
  exposed yet, and old sessions without a run show an idle goal bar.
- OIDC tenant switching and multi-environment profiles are not implemented yet;
  the current SSO session restores at launch, locks after idle and signs out on
  `Disconnect`.
- Theme preference is stored in `localStorage` only. Language is persisted by
  `SetLanguage` when explicitly chosen and mirrored to `localStorage`.
- The bridge commands do not expose an idempotency key. The UI never retries
  writes automatically and requires re-confirmation for action decisions, but a
  transport-level retry (user-triggered) can submit a new request after an
  unknown first outcome; the server remains authoritative.
- Streaming deltas are best effort. If deltas are dropped, the durable
  projection is re-fetched; the frontend keeps cursors in memory and reconnects
  from the last seen sequence.
- Remote-URL images render through the WebView with `referrerPolicy="no-referrer"`;
  stored `ref` attachments are fetched through the Go bridge. Links/files still
  open through `OpenURL`.
- The transcript is not virtualized yet; very long sessions rely on browser
  scrolling.
- `dist-next/` is embedded by `desktop/assets_next.go` with the `nextui` build tag;
  the legacy `desktop/frontend/dist/` stays untouched.
