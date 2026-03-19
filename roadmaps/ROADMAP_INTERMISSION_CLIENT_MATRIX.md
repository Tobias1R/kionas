# Intermission Client Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): "Build a decent client to read inputs and display data;".

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Input-driven interactive client mode (REPL) | Done | [client/src/main.rs](client/src/main.rs) | Client now runs an interactive prompt (`kionas>`) and reads SQL input line by line. |
| One-shot query execution mode | Done | [client/src/main.rs](client/src/main.rs) | Added `--query` mode for single-query execution without entering REPL loop. |
| Authenticated query dispatch flow retained | Done | [client/src/main.rs](client/src/main.rs) | Login/auth metadata flow preserved and reused by interactive and one-shot paths. |
| Flight decode and human-readable display | Done | [client/src/main.rs](client/src/main.rs), [client/Cargo.toml](client/Cargo.toml) | DoGet stream is decoded to Arrow batches and rendered as table output with row/column/batch counts. |
| Proxy-routed structured handle consumption | Done | Runtime evidence from manual run: `flight://kionas-flight-proxy:443/...` | SELECT flow returns structured handle and retrieves data through flight-proxy. |
| End-to-end manual validation (create/schema/table/insert/select) | Done | User runtime logs from manual session on 2026-03-19 | Verified successful interactive lifecycle and tabular SELECT output (`rows=4 columns=2 batches=1`). |
| Session persistence/history/completion UX hardening | Deferred | N/A | Explicitly out of current intermission scope; candidate follow-up improvement. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Intermission client signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. Intermission mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.
