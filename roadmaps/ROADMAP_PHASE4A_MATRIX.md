# Phase 4A Completion Matrix

## Scope
Slice A from [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): roadmap items 5.1 and 5.2 (Flight proxy service endpoints and worker Flight surface completion).

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 5.1 Proxy service no longer bootstrap-only | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs), [flight_proxy/Cargo.toml](flight_proxy/Cargo.toml) | Proxy now starts tonic Flight server and forwards DoGet to worker Flight endpoint. |
| 5.1 Proxy validation boundary for ticket shape | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs) | DoGet now validates internal ticket encoding and scope format before forwarding. |
| 5.1 Proxy GetFlightInfo support | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs) | Proxy now forwards GetFlightInfo to worker using descriptor-scoped worker routing. |
| 5.1 Proxy GetSchema support | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs) | Proxy now forwards GetSchema to worker using descriptor-scoped worker routing. |
| 5.2 Worker GetFlightInfo implementation | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs) | Returns metadata-backed FlightInfo with schema bytes, endpoint, ticket, and totals. |
| 5.2 Worker GetSchema implementation | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs) | Returns schema payload for task-scoped descriptor after metadata-backed validation path. |
| 5.2 Worker ListFlights implementation | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs) | Returns empty stream instead of unimplemented (safe minimal behavior). |
| 5.2 Worker RPC GetFlightInfo schema no longer stubbed | Done | [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs) | Schema field now sourced from result metadata sidecar columns. |
| Slice A manual container proof: query -> proxy DoGet -> client decode | Not Started | N/A | Pending user-run container verification. |
| Slice A regression check for create/insert/query baseline | Not Started | N/A | Pending validation cycle. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase 4A signoff: `Pending`
- Blocking items:
  - Manual container validation and regression evidence

## Gate Checklist
1. All mandatory Slice A criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.
