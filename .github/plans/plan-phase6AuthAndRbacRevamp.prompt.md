## Plan: Phase 6 Auth and RBAC Revamp

Implement Phase 6 by adding end-to-end auth context propagation through gRPC metadata, enforcing authorization in worker query and Flight paths, introducing signed Flight tickets, and applying bounded-staleness revocation checks. Keep scope intentionally limited to query execution and Flight retrieval paths (exclude transaction RPC auth in this phase).

**Project Infra**
- REDIS is available. All connections must be pooled.
- Postgres is available. All connections must be pooled.

**Steps**
1. Phase 6A: Contract and Architecture Freeze
1.1 Confirm Phase 6 scope in roadmap terms: 7.1 to 7.4 only for query dispatch/execution/Flight retrieval, excluding Prepare/Commit/Abort enforcement for this slice.
1.2 Define canonical metadata contract for propagation across services: `authorization` bearer token, `session_id`, and optional trace correlation metadata.
1.3 Define centralized server-side authorization decision object (resource + action + allowed scopes) that worker consumes as trusted scope context, without worker live-calling metastore for RBAC.
1.4 Define bounded-staleness revocation policy (TTL window + revalidation points) for worker and flight proxy/worker Flight boundaries.
1.5 Create a provider to operate the backend to persist session and role data, issue tokens, and manage revocation state (can be in-memory for Phase 6 but should have clear interface for future externalization).
1.6 Document expected auth flow and contracts in `docs/PHASE6_AUTH.md` for internal reference and onboarding.

2. Phase 6B: Server-Side Context Propagation and Scope Computation
2.1 Thread `RequestContext` from warehouse service entry through statement handling and task dispatch helpers so auth/session context is available at all query dispatch points.
2.2 Add explicit query-scope computation on server at dispatch time (for example: table-level SELECT scope based on parsed plan namespace + role bindings).
2.3 Attach computed scope context to outbound worker calls using gRPC metadata only (no proto field expansion in this phase).
2.4 Ensure existing payload/task protocol remains backward compatible and does not regress Phase 5 distributed execution behavior.
2.5 Add debug-level explainability hooks for computed scope context to aid troubleshooting without leaking sensitive tokens.

3. Phase 6C: Worker Enforcement for Query Execution
3.1 Add inbound metadata extraction/validation utility at worker service boundaries: parse bearer token, read session_id, validate signature/expiry, and bind to scope context.
3.2 Enforce authorization before physical query execution starts (deny-by-default if metadata missing, invalid, expired, or scope insufficient).
3.3 Add bounded-staleness revocation check cache strategy in worker (TTL-based) and deterministic fallback behavior when auth backend/session source is unavailable.
3.4 Ensure error responses are actionable and consistent with existing response patterns (authorization denied vs internal error separation).

4. Phase 6D: Flight Layer Hardening
4.1 Replace current structural internal ticket format with signed ticket format (HMAC/JWT-like) carrying minimally required claims: session_id, task_id, worker_id, expiry, and scope hash/version.
4.2 Add ticket issue/verify helpers and use them in worker Flight `GetFlightInfo`, `GetSchema`, and `DoGet` paths.
4.3 Enforce session/token validity and scope checks in Flight endpoints with bounded-staleness revalidation.
4.4 Update flight proxy to preserve/forward auth metadata end-to-end so retrieval remains authorized and traceable.

5. Phase 6E: Verification and Signoff Artifacts
5.1 Add/extend unit tests for: metadata extraction, token validation failures, scope-denied execution, signed ticket verification, and revocation-window behavior.
5.2 Add integration tests for happy and deny flows: server query dispatch -> worker execution -> flight retrieval, including revoked/expired token scenarios.
5.3 Run required quality gates after changes: `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo check` (Windows flow via docker when needed).
5.4 Create and maintain `roadmaps/ROADMAP_PHASE6_MATRIX.md` with mandatory criteria mapped to 7.1 to 7.4, evidence links, deferred hardening items, and final signoff decision.

6. Dependency and Parallelism Map
6.1 6A blocks 6B/6C/6D because metadata/ticket/revocation contracts must be stable first.
6.2 6B blocks 6C because worker enforcement needs server-computed scope and propagated metadata.
6.3 6C and 6D can run partially in parallel after 6A, but final integration requires both complete.
6.4 6E starts with unit tests in parallel as slices land; final integration and signoff gating blocks completion.

**Relevant files**
- `c:/code/kionas/roadmaps/ROADMAP.md` — source phase scope (items 7.1 to 7.4) and dependency references.
- `c:/code/kionas/server/src/services/request_context.rs` — request auth/session context model.
- `c:/code/kionas/server/src/services/warehouse_service_server.rs` — gRPC query entrypoint where context is extracted and routed.
- `c:/code/kionas/server/src/statement_handler/mod.rs` — statement routing seam to propagate context.
- `c:/code/kionas/server/src/statement_handler/query_select.rs` — query dispatch path to bind scopes.
- `c:/code/kionas/server/src/statement_handler/helpers.rs` — worker dispatch helpers where metadata attachment should be centralized.
- `c:/code/kionas/server/src/auth/jwt.rs` — token validation expectations and claims model alignment.
- `c:/code/kionas/server/src/session.rs` — session lifecycle and role/session data source.
- `c:/code/kionas/worker/src/services/worker_service_server.rs` — query RPC enforcement entrypoint.
- `c:/code/kionas/worker/src/services/query_execution.rs` — pre-execution authorization gate and denial behavior.
- `c:/code/kionas/worker/src/flight/server.rs` — signed ticket lifecycle and Flight auth checks.
- `c:/code/kionas/flight_proxy/src/main.rs` — metadata forwarding for Flight calls.
- `c:/code/kionas/kionas/proto/worker_service.proto` — reference only for compatibility verification (no required field additions in selected approach).
- `c:/code/kionas/roadmaps/ROADMAP_PHASE_MATRIX_TEMPLATE.md` — signoff matrix format baseline.

**Verification**
1. Unit tests: metadata parsing and auth guard helpers in server/worker/flight modules.
2. Unit tests: signed ticket issue/verify and claim mismatch rejection.
3. Integration tests: authorized flow for query and Flight retrieval with expected success.
4. Integration tests: unauthorized role, expired token, revoked token, and missing metadata produce expected denial codes/messages.
5. Regression validation: create/insert/query baseline remains green from earlier phases.
6. Quality gates: run `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo check`.
7. Matrix gate: `roadmaps/ROADMAP_PHASE6_MATRIX.md` has all mandatory criteria `Done` with concrete evidence before signoff accepted.

**Decisions**
- Auth propagation: gRPC metadata only.
- Authorization source: server computes scopes; worker enforces trusted scopes.
- Ticket model: signed tickets for Flight.
- Revocation policy: bounded staleness.
- Phase cut: include query + Flight paths; exclude transaction RPC auth checks from Phase 6.

**Further Considerations**
1. Scope claim format: choose minimal stable schema (resource/action list) to avoid frequent contract churn in later phases.
2. Key management: define where ticket-signing key is loaded/rotated (config seam) to support future rotation without protocol breakage.
3. Failure mode policy: decide strict fail-closed behavior when revocation backend is unavailable vs temporary grace within TTL window.
