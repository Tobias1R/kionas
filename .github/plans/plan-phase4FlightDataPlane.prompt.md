## Plan: Phase 4 Flight Data Plane

Implement Phase 4 as two signoff slices to reduce risk and unblock retrieval gateway adoption: Slice A covers roadmap items 5.1-5.2 (proxy service + worker Flight endpoint completeness), Slice B covers 5.3-5.4 (ticket lifecycle baseline + end-to-end validation). Keep ticket lifecycle scope baseline in this phase (scoping/validation + TTL-backed expiration behavior), with strict replay controls deferred.

**Steps**
1. Phase A1: Contract and Scope Lock (blocks all later work)
1.1 Anchor scope to [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md) Phase 4 only, with explicit split: Slice A = 5.1-5.2, Slice B = 5.3-5.4.
1.2 Preserve existing DoGet ticket shape session_id:task_id:worker_id and compatibility with current client/server helpers.
1.3 Define explicit defer list for Phase 4: cryptographic ticket signatures, strict replay single-use controls, and deep revocation semantics.

2. Phase A2: Flight Proxy Service Implementation (depends on 1)
2.1 Replace bootstrap-only proxy in [flight_proxy/src/main.rs](flight_proxy/src/main.rs) with tonic Flight server startup, config loading via kionas config path, and logging/TLS wiring consistent with workspace patterns.
2.2 Implement proxy handler path for DoGet forwarding in new proxy modules, using ticket decode + worker resolution + stream relay.
2.3 Reuse worker discovery/channel patterns from [server/src/warehouse/state.rs](server/src/warehouse/state.rs) and [server/src/workers_pool.rs](server/src/workers_pool.rs) to avoid divergent connection behavior.
2.4 Add request validation boundary in proxy (malformed ticket, worker mismatch signals, missing task/result location mapping).

3. Phase A3: Worker Flight Surface Completion (depends on 2 in integration, can start in parallel after 1)
3.1 Implement GetFlightInfo in [worker/src/flight/server.rs](worker/src/flight/server.rs) using persisted result metadata and stable endpoint/ticket construction.
3.2 Implement GetSchema in [worker/src/flight/server.rs](worker/src/flight/server.rs) with Arrow IPC schema bytes built from validated metadata.
3.3 Implement ListFlights in [worker/src/flight/server.rs](worker/src/flight/server.rs) with minimal safe listing semantics (session/task scoped or explicit empty behavior if no listing source available).
3.4 Replace stubbed worker RPC flight info response path in [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs) so schema and ticket fields are generated from real metadata.

4. Phase A4: Slice A Verification and Gate (depends on 2-3)
4.1 Manual container proof: query dispatch -> worker execution -> proxy retrieval via DoGet -> client decode.
4.2 Manual container proof: GetFlightInfo/GetSchema return non-unimplemented responses and are consumable by client tooling.
4.3 Regression proof: create/insert/query baseline still stable.
4.4 Produce and sign off [roadmaps/ROADMAP_PHASE4A_MATRIX.md](roadmaps/ROADMAP_PHASE4A_MATRIX.md) with mandatory Done/Deferred evidence.

5. Phase B1: Ticket Lifecycle Baseline (depends on Slice A)
5.1 Standardize ticket semantics document-in-code: scoping guarantees (session/task/worker), validation checkpoints, and TTL-backed expiration expectations.
5.2 Keep lifecycle baseline implementation aligned with existing Redis TTL behavior in worker state (no strict replay lockout in this phase).
5.3 Add clear status-code mapping for invalid ticket format, wrong worker scope, and expired/missing result mapping.

6. Phase B2: End-to-End Validation Harness (depends on 5)
6.1 Add integration coverage for server query -> worker execution -> proxy retrieval -> client decode (or equivalent orchestrated test harness where feasible).
6.2 Include negative-path validations: malformed ticket and worker-scope mismatch.
6.3 Manual container validation remains acceptable evidence for this phase; automated coverage can be partial if environment constraints block full orchestration.
6.4 Produce and sign off [roadmaps/ROADMAP_PHASE4B_MATRIX.md](roadmaps/ROADMAP_PHASE4B_MATRIX.md) with mandatory Done/Deferred evidence.

7. Phase B3: Final Phase 4 Consolidation (depends on Slice A and Slice B matrices)
7.1 Publish final consolidated signoff file [roadmaps/ROADMAP_PHASE4_MATRIX.md](roadmaps/ROADMAP_PHASE4_MATRIX.md) summarizing Slice A/B outcomes and deferred hardening.
7.2 Confirm deferred items are explicitly tagged non-blocking and mapped to later phases (primarily Phase 6 and Phase 7).

**Parallelism and Dependencies**
1. A1 must complete first.
2. A2 and A3 can develop in parallel after A1, but Slice A signoff requires both complete.
3. B1 and B2 start after Slice A signoff.
4. B3 is final closure step after both slice matrices are accepted.

**Relevant files**
- [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md) - authoritative Phase 4 scope after file move.
- [flight_proxy/src/main.rs](flight_proxy/src/main.rs) - proxy bootstrap to full server implementation.
- [worker/src/flight/server.rs](worker/src/flight/server.rs) - DoGet plus missing GetFlightInfo/GetSchema/ListFlights.
- [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs) - flight info RPC currently stub-like schema path.
- [server/src/warehouse/state.rs](server/src/warehouse/state.rs) - worker resolution patterns to reuse.
- [server/src/workers_pool.rs](server/src/workers_pool.rs) - worker channel pooling patterns to reuse.
- [worker/src/state/mod.rs](worker/src/state/mod.rs) - task result location and TTL behavior for baseline lifecycle semantics.
- [client/src/main.rs](client/src/main.rs) - retrieval compatibility and handle-driven verification path.

**Verification**
1. Slice A manual checks in container stack:
1.1 Submit supported SELECT and capture handle.
1.2 Resolve through proxy and retrieve result stream successfully.
1.3 Call GetFlightInfo and GetSchema through implemented paths and verify non-stub outputs.
2. Slice B manual checks:
2.1 Validate malformed ticket returns clear failed-precondition/invalid-argument style error mapping.
2.2 Validate wrong worker scope is denied.
2.3 Validate expired or missing mapping behavior follows TTL-backed not-found semantics.
3. Quality gates before each slice signoff (run where environment supports): cargo fmt --all, cargo clippy --all-targets --all-features -- -D warnings, cargo check, cargo test -- --test-threads=1.
4. Matrix gates: each slice must have mandatory items Done with concrete evidence links before final Phase 4 signoff.

**Decisions**
- User-selected scope split: Slice A (5.1-5.2) then Slice B (5.3-5.4).
- Ticket lifecycle depth for Phase 4 is baseline, not strict replay-hardening.
- Manual container validation is acceptable for signoff evidence in this phase.
- ROADMAP path source is [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md), not root ROADMAP.md.

**Further Considerations**
1. Decide whether ListFlights should be minimal empty-safe behavior for now or session-task listing from cache in Slice A.
2. Decide whether proxy should enforce JWT at launch in Slice A or keep auth passthrough and harden in Phase 6.
3. If full automated e2e remains costly, add a stable scripted harness to reduce manual drift across contributors.
