## Plan: Distributed Query Engine Roadmap

Build from the current working query-dispatch foundation into a full distributed query engine in staged, verifiable milestones: SQL AST intake is already live, operation=query dispatch is live, worker query handle stub is live, and structured query handle delivery is live. Next phases introduce logical planning, physical planning, Flight data plane, auth hardening, and coordinated distributed execution so more developers can contribute in parallel without architecture churn.

**Steps**
1. Phase 0: Baseline and Story So Far (done, keep stable)
1.1 Keep the current minimal SELECT intake and canonical payload contract stable while we expand internals.
1.2 Preserve the current server outcome contract RESULT|category|code|message to avoid breaking clients during roadmap execution.
1.3 Lock in regression checks for create/insert/query dispatch paths before major planner work begins.
[X] DONE

2. Phase 1: Query Semantics Contract (AST -> Logical Plan)
2.1 Introduce an explicit logical plan model as the first-class internal representation for SELECT (scan/project/filter initially).
2.2 Replace flat canonical payload content with a versioned logical-plan payload envelope while maintaining backward compatibility for version 1 payloads.
2.3 Add deterministic logical-plan validation and normalization rules (identifier casing, namespace resolution, projection/selection canonicalization).
2.4 Add explainability hooks (logical plan text/JSON in debug paths) for onboarding and troubleshooting.
[X] DONE

3. Phase 2: Physical Planning (Logical -> Physical)
3.1 Define a physical operator model for local execution (table scan, predicate filter, projection, materialize).
3.2 Implement logical-to-physical translation with clear operator boundaries and future distribution metadata placeholders.
3.3 Add a capability matrix so unsupported operators fail fast with validation-grade responses instead of infra-style errors.
3.4 Add planner benchmarks and complexity guardrails to keep functions under repository complexity thresholds.
[X] DONE

4. Phase 3: Local Query Execution on Worker
4.1 Evolve query stub into local execution for single-table SELECT by reading Delta data, applying filter/project, and producing Arrow batches.
4.2 Persist deterministic query artifacts per task for retrieval and replay safety.
4.3 Populate result metadata (row count, schema summary, result location) in a stable shape for server and proxy consumption.
4.4 Keep insert/create paths untouched except where shared execution utilities are intentionally reused.
[X] DONE

5. Phase 4: Flight Data Plane and Proxy
5.1 Implement flight_proxy service endpoints beyond bootstrap so clients can resolve and retrieve task results through a stable gateway.
5.2 Complete worker Flight service surface needed by proxy and clients (GetFlightInfo/GetSchema/ListFlights priority after DoGet).
5.3 Standardize ticket format and lifecycle (session/task/worker scoping, expiration, validation, replay constraints).
5.4 Add end-to-end integration tests: server query -> worker execution -> proxy retrieval -> client decode.

[INTERMISSION]
Build a decent client to read inputs and display data;

6. Phase 5: Distributed Physical Plan and Task Graph
6.1 Introduce distributed physical plan primitives (stage graph, exchange/shuffle boundaries, partition specs).
6.2 Compile distributed physical plan into task DAGs routed through existing TaskManager and worker pools.
6.3 Add worker coordination protocol for stage dependencies, completion signaling, and retry-safe progress updates.
6.4 Implement first distributed use-case slice (parallel scan + merge) before joins/aggregations.

7. Phase 6: Authentication and Authorization Revamp
7.1 Propagate auth context explicitly through query execution artifacts where needed (server dispatch, worker execution, Flight retrieval).
7.2 Add worker-side enforcement points for query execution authorization and scope checks.
7.3 Add Flight-layer auth validation and token/session checks in proxy and worker endpoints.
7.4 Add token lifecycle and revocation strategy appropriate for long-running distributed queries.

8. Phase 7: Reliability, Observability, and Developer Experience
8.1 Add query lifecycle telemetry (parse/planner/execution timings, stage counters, retry/failure metrics).
8.2 Add structured audit trail across server dispatch, worker execution, and Flight retrieval.
8.3 Add failure-injection and recovery tests for worker loss, partial stage failure, and retry paths.
8.4 Publish contributor tracks and “good first milestones” tied to concrete files and tests to attract parallel development.

9. Phase 8: Feature Expansion
9.1 Add joins, aggregates, ordering, and group-by in logical/physical planning after distributed foundation is stable.
9.2 Add cost-based or heuristic optimization passes only after plan correctness and observability are proven.
9.3 Expand query federation and advanced semantics iteratively with feature flags.

10. Phase 9: Server session expansion



**Parallelism and Dependencies**
1. Phase 1 blocks Phases 2 and 3 because logical plan contract must stabilize first.
2. Phase 4 can start partially in parallel with late Phase 3, but full proxy integration depends on real worker query output.
3. Phase 5 depends on Phases 2 through 4.
4. Phase 6 can begin early for context propagation design, but enforcement and Flight checks depend on Phase 4 and Phase 5 interfaces.
5. Phase 7 runs continuously and deepens after each phase.

**Achievements So Far**
1. Server query dispatch path exists for SELECT via statement handler routing and canonical payload creation.
2. Query tasks are dispatched with operation=query and stable params through shared task helper flow.
3. Worker operation=query branch exists and returns deterministic query handle location.
4. Query handle now propagates in structured response data (not only free-text message).
5. Logging was compacted to remove oversized AST payload dumps from info logs.

**Contributor Tracks**
1. Planner track: logical plan model, validation, explain output.
2. Execution track: worker local scan/filter/project and deterministic materialization.
3. Flight track: proxy implementation, ticket lifecycle, schema/info endpoints.
4. Distributed systems track: stage DAG compiler, scheduler, worker coordination.
5. Security track: auth propagation, worker/proxy authorization, token lifecycle.
6. Reliability track: retries, cancellation, telemetry, audit, chaos tests.

**Relevant Files**
- c:/code/kionas/server/src/statement_handler/query_select.rs - current AST normalization and query dispatch contract.
- c:/code/kionas/server/src/statement_handler/mod.rs - central statement routing.
- c:/code/kionas/server/src/statement_handler/helpers.rs - task orchestration and worker dispatch path.
- c:/code/kionas/server/src/services/warehouse_service_server.rs - response mapping and structured handle propagation.
- c:/code/kionas/server/src/tasks/mod.rs - TaskManager and task lifecycle model.
- c:/code/kionas/server/src/workers/mod.rs - worker communication and dispatch helpers.
- c:/code/kionas/worker/src/transactions/maestro.rs - operation dispatch and execution entry.
- c:/code/kionas/worker/src/services/query.rs - query payload validation and result-location contract.
- c:/code/kionas/worker/src/flight/server.rs - worker flight service implementation surface.
- c:/code/kionas/flight_proxy/src/main.rs - proxy bootstrap currently awaiting endpoint implementation.
- c:/code/kionas/kionas/src/parser/sql.rs - SQL parsing entrypoint.
- c:/code/kionas/kionas/proto/worker_service.proto - worker task and flight info RPC contract.
- c:/code/kionas/kionas/proto/warehouse_service.proto - client-facing query response contract.
- c:/code/kionas/server/src/auth_setup.rs - auth initialization seam.
- c:/code/kionas/server/src/auth/jwt.rs - server-side token interception.
- c:/code/kionas/server/src/services/request_context.rs - request auth/session context extraction.

**Verification**
1. Maintain a phase-gated integration suite where each phase adds one new passing end-to-end scenario without regressing create/insert/query baseline.
2. Run repository-required quality gates on each milestone: cargo fmt --all, cargo clippy --all-targets --all-features -- -D warnings, cargo check, cargo test -- --test-threads=1.
3. On Windows, use docker-based cargo check path for server/worker crates when native toolchain issues occur.
4. Add milestone acceptance demos for contributors: query dispatch demo, local execution demo, proxy retrieval demo, distributed stage demo.

**Decisions**
- Include in roadmap: AST->logical->physical, auth revamp, flight proxy, distributed plan execution, worker coordination, and observability.
- Keep current contracts stable where possible to avoid blocking parallel team contributions.
- Use versioned payload/protocol transitions to allow incremental rollout.
- Defer broad SQL feature expansion until distributed foundations are stable.
