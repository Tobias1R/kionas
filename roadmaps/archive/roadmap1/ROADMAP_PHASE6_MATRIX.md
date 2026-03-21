# Phase 6 Completion Matrix

## Scope
Phase 6 from [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md):
- 7.1 Propagate RBAC auth context through query execution artifacts (server dispatch, worker execution, Flight retrieval).
- 7.2 Add worker-side authorization enforcement and scope checks.
- 7.3 Add Flight-layer auth validation and token/session checks in proxy endpoints.
- 7.4 Add token lifecycle and revocation strategy suitable for long-running distributed queries.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 7.1 Server-to-worker auth context propagation over metadata | Done | [server/src/auth/jwt.rs](server/src/auth/jwt.rs), [server/src/services/request_context.rs](server/src/services/request_context.rs), [server/src/statement_handler/query_select.rs](server/src/statement_handler/query_select.rs), [server/src/workers/mod.rs](server/src/workers/mod.rs) | JWT subject is propagated as `rbac_user`; server computes `auth_scope`; dispatch forwards metadata to worker. |
| 7.2 Worker-side query authorization enforcement | Done | [worker/src/authz.rs](worker/src/authz.rs), [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Worker validates token/session/role/scope before query execution and denies missing/invalid context. |
| 7.3 Flight-layer auth and proxy metadata forwarding | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs), [flight_proxy/src/main.rs](flight_proxy/src/main.rs), [worker/src/services/query.rs](worker/src/services/query.rs), [client/src/main.rs](client/src/main.rs) | Worker Flight verifies signed tickets; proxy forwards auth metadata; query handle carries signed ticket; client consumes signed ticket when present. |
| 7.4 Token lifecycle and bounded-staleness revocation strategy | Done | [worker/src/authz.rs](worker/src/authz.rs), [docs/PHASE6_AUTH.md](docs/PHASE6_AUTH.md) | In-memory provider with revocation cache TTL (`AUTH_REVOCATION_CACHE_TTL_SECONDS`) and signed ticket expiry (`FLIGHT_TICKET_TTL_SECONDS`). |
| Transaction RPC auth checks (Prepare/Commit/Abort) | Deferred | [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md) | Explicitly excluded by agreed Phase 6 scope cut. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected:
  - `JWT_SECRET`: used by [worker/src/authz.rs](worker/src/authz.rs) for JWT validation and signed ticket verification.
  - `FLIGHT_TICKET_TTL_SECONDS`: used by [worker/src/authz.rs](worker/src/authz.rs) for signed ticket expiry.
  - `AUTH_REVOCATION_CACHE_TTL_SECONDS`: used by [worker/src/authz.rs](worker/src/authz.rs) for bounded-staleness revocation cache.
  - `FLIGHT_PROXY_HOST` and `FLIGHT_PROXY_PORT`: used by [worker/src/services/query.rs](worker/src/services/query.rs) for retrieval endpoint generation.
- Parameters expected:
  - Metadata keys `authorization`, `session_id`, `rbac_user`, `rbac_role`, `auth_scope`, `query_id` are validated and propagated in [server/src/workers/mod.rs](server/src/workers/mod.rs), [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs), and [flight_proxy/src/main.rs](flight_proxy/src/main.rs).
