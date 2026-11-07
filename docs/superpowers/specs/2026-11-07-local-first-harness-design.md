# Local-First Harness Design

**Status:** Phase 1 shipped; Phase 2 shipped  
**Date:** 2026-11-07  
**Scope:** downloadable desktop client that runs the harness locally without Docker, PostgreSQL, NATS or a CLI.

> **Update:** Phase 1 used an embedded PostgreSQL child process as a bridge. Phase 2 replaced it with `internal/localstore` (SQLite, pure Go) and `cmd/harness-local`; local mode no longer starts or bundles PostgreSQL. Execution events are append-only JSONL (`events/local/<run>.jsonl`), while SQLite stores state and byte-range event indexes; `RebuildEventIndex` can rebuild the index from JSONL. The local runtime also implements the same `/v1/extensions` API (models, MCP, HTTP tools, skills, web tools, tool toggles) and the full command set. The embedded-PostgreSQL sections below describe the historical Phase 1 design; the runtime contract (same `/v1` client, local token, data directory, remote team mode) is unchanged.

## Problem

The current harness is server-first: the desktop client is a thin `/v1` client, while runs need PostgreSQL, a worker, the scheduler and optionally NATS. A user cannot download the client, double-click it and use the agent without starting infrastructure.

DeepSeek Harness and Codex solve this by shipping a local runtime:

- DeepSeek Harness: local web UI/CLI, plugin-composed runtime, append-only session log, local sandbox/storage, remote model API.
- Codex: a Rust core + app-server protocol; every surface (CLI, IDE, desktop, cloud) is a client of the same core; local sandbox and approval policies; local sessions.

We want the same user experience while keeping the existing durable step engine, flow DSL, plan review, approvals and audit semantics.

## Goals

1. Download the desktop app and run it with no separate installation or process to start.
2. Keep the existing `/v1` API and UI; local and remote modes use the same client code.
3. Local mode stores data under the application data directory.
4. Local mode does not require Docker, a system PostgreSQL, NATS or a terminal.
5. The same app can connect to a team server for collaboration.
6. Approvals, plan hash binding, tool risk and audit remain server-enforced in both modes.
7. Borrow the useful parts of dsh/Codex:
   - local runtime owned by the client (dsh/Codex)
   - a stable core/UI boundary (Codex app-server)
   - append-only run/event history and replay (dsh; already present in PostgreSQL)

## Non-goals (Phase 1)

- Multi-device session sync or merging local and remote histories.
- Replacing PostgreSQL with SQLite. This is Phase 2.
- A full plugin marketplace or arbitrary runtime plugin loading.
- Running models fully offline. Models still use a remote provider unless the user configures a local OpenAI-compatible server.

## Architecture

```text
Desktop (Wails)
  │  starts and supervises
  ▼
Local Runtime Manager
  ├── embedded PostgreSQL process (loopback, random port)
  ├── harness core process (`harness all`, loopback, random port)
  │     ├── API + Worker + Scheduler
  │     ├── local event bus (no NATS)
  │     ├── filesystem artifacts
  │     └── local SQL? no — PostgreSQL data directory
  └── random bearer token passed to both sides

Desktop UI ── HTTP/SSE /v1 ──> local harness core
Desktop UI ── HTTP/SSE /v1 ──> remote team harness (remote mode)
```

The desktop process owns the local core lifecycle. The user sees one application; PostgreSQL and the core are child processes.

Why embedded PostgreSQL first:

- Reuses the complete existing store, RLS semantics, flow engine, plan review, eval and migrations.
- No duplicate storage implementation and no semantic drift.
- Delivers the local-first experience without a multi-week store rewrite.

Phase 2 introduces a storage abstraction and a SQLite/local store to reduce package size and remove the PostgreSQL child process. The client/core boundary and deployment model do not change.

## Local data layout

```text
macOS:   ~/Library/Application Support/SRE Harness/
Linux:   ~/.local/share/sre-harness/
Windows: %LOCALAPPDATA%\SRE Harness\

  pgdata/          PostgreSQL data directory
  artifacts/       local artifact store
  logs/            core and PostgreSQL logs
  runtime.json     port, pid and shutdown metadata (0600)
  tokens.json      SHA-256 of the random local token (0600)
```

## Local security

- PostgreSQL and the harness core listen on `127.0.0.1` only.
- The harness core uses token auth, not dev header auth.
- A random 32-byte token is generated per launch; only its SHA-256 is written to disk.
- The data directory is created with mode `0700`; token/settings files use `0600`.
- The desktop client keeps the token in memory.
- No NATS, no public listener, no default credentials.
- Approval and plan-hash checks remain in the core, not the UI.

## Remote team mode

The same desktop app has two connection profiles:

- **Local**: start/supervise the local core and connect to `http://127.0.0.1:<port>`.
- **Team**: connect to `https://harness.example.com` with OIDC or a bearer token.

The UI, run view, flow view, plan review and approvals are identical. Only the transport endpoint and credential source differ.

## Acceptance criteria

1. On a clean machine with the packaged app, local mode starts without Docker/PostgreSQL/NATS/CLI.
2. The app can create a session, run the scripted model, execute a flow, approve a plan and complete a run.
3. Closing the app stops the core and PostgreSQL cleanly.
4. A crash/restart reuses the existing data directory and resumes the queue.
5. No listener is bound outside loopback.
6. Remote mode still connects to the existing team server with a bearer token; the OIDC PKCE login flow is Phase 3.
7. The local token is not accepted after restart.
8. Package size and startup time are measured and documented.