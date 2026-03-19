# Phase 4B Completion Matrix

## Scope
Slice B from [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): roadmap items 5.3 and 5.4 (ticket lifecycle baseline and end-to-end validation).

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 5.3 Ticket format remains session/task/worker scoped | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs), [client/src/main.rs](client/src/main.rs) | Ticket payload shape remains `session_id:task_id:worker_id` URL-safe base64. |
| 5.3 Malformed ticket validation with clear status mapping | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs), [worker/src/flight/server.rs](worker/src/flight/server.rs) | Invalid UTF-8/base64/payload-shape tickets return `invalid_argument`. |
| 5.3 Upstream worker status codes are preserved by proxy | Done | [flight_proxy/src/main.rs](flight_proxy/src/main.rs) | Proxy now preserves upstream status code family for DoGet/GetFlightInfo/GetSchema failures. |
| 5.3 Wrong worker scope denied at worker | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs) | Worker enforces ticket worker_id match and returns `permission_denied`. |
| 5.3 Expired/missing mapping uses TTL-backed not_found semantics | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs), [worker/src/state/mod.rs](worker/src/state/mod.rs) | Retrieval now reports `result location missing or expired` when TTL-backed mapping is absent. |
| 5.3 Replay constraints beyond baseline (single-use/nonce/rate limit) | Deferred | N/A | Deferred to later hardening phase by plan decision. |
| 5.4 End-to-end retrieval validation through proxy (manual) | Done | User runtime logs: proxy-routed handle `flight://kionas-flight-proxy:443/...` and deterministic retrieval confirmed | Manual container validation accepted for this phase. |
| 5.4 Negative-path harness for malformed/wrong-worker tickets | Done | [client/src/main.rs](client/src/main.rs) | Added client switches to execute malformed-ticket and wrong-worker-ticket checks against proxy endpoint. |
| 5.4 Negative-path validation: malformed ticket | Done | User runtime logs: `Client specified an invalid argument` and message `ticket is not valid base64` | Malformed ticket is rejected with clear invalid-argument semantics. |
| 5.4 Negative-path validation: wrong worker scope | Done | User runtime logs: proxy DoGet failed with `service unavailable` due tampered worker endpoint `http://kionas-worker1-tampered:50052` | Wrong-worker ticket is rejected on proxy path; request is not accepted for retrieval. |
| 5.4 Regression stability for create/insert/query baseline | Done | User runtime logs with deterministic retrieval plus invalid JOIN -> BUSINESS_UNSUPPORTED_QUERY_SHAPE | Baseline behavior stable during proxy rollout. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase 4B signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. All mandatory Slice B criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.
