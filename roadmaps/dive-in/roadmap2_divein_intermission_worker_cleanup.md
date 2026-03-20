# Roadmap2 Intermission Dive-In: Worker Query Runtime Cleanup

## Scope
Intermission scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Break down [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/services/query.rs](worker/src/services/query.rs), and prior join runtime placement from services.
- Propose and validate a new module home for query runtime logic outside services.
- Start incremental implementation with safe, compile-checked migration steps.

## Decision
- New module home: top-level execution domain under [worker/src/execution](worker/src/execution).
- Boundary rule:
1. [worker/src/services](worker/src/services) remains transport-facing (gRPC/request adapters and thin entry points).
2. [worker/src/execution](worker/src/execution) owns query runtime/domain logic.

## Current Responsibility Baseline
1. [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs): runtime orchestration, pipeline ops, stage exchange I/O, result artifact persistence.
2. [worker/src/services/query.rs](worker/src/services/query.rs): namespace extraction/validation, payload shape checks, query task stub entry.
3. [worker/src/services/query_join.rs](worker/src/services/query_join.rs): hash join runtime algorithm (now moved).
4. [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs): task dispatch orchestration.
5. [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs): transport endpoint.

## Proposed Module Structure
Under [worker/src/execution](worker/src/execution):
1. [worker/src/execution/query.rs](worker/src/execution/query.rs)
- Namespace and payload validation.
- Query task stub entry during transition.

2. [worker/src/execution/join.rs](worker/src/execution/join.rs)
- Join algorithm and join-specific helpers.

3. Future modules in this intermission:
- `planner.rs`: runtime-plan extraction and stage-context parsing.
- `pipeline.rs`: operator sequencing and orchestration.
- `artifacts.rs`: parquet/result/exchange persistence helpers.

## Migration Strategy (Incremental)
1. Step 1 completed:
- Add [worker/src/execution/mod.rs](worker/src/execution/mod.rs).
- Move query + join modules into execution.
- Keep compatibility via [worker/src/services/query.rs](worker/src/services/query.rs) shim for dispatch stability.
- Update [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) imports to execution paths.

2. Next steps:
- Extract artifact persistence helpers from query_execution to execution/artifacts.
- Extract runtime plan/stage loading to execution/planner.
- Move orchestration into execution/pipeline.
- Remove remaining query runtime files from services once call-sites are fully cut over.

## Risks and Mitigations
1. Risk: import cycles while splitting runtime internals.
- Mitigation: move low-coupling modules first, then extract helpers in one-way dependency order.

2. Risk: behavior drift during refactor.
- Mitigation: no semantic changes during cleanup; compile-check after each migration step.

3. Risk: hidden call-site dependencies.
- Mitigation: preserve temporary shim entry points in services until final cut-over.

## Initial Implementation Evidence
1. New execution module scaffold and moved files:
- [worker/src/execution/mod.rs](worker/src/execution/mod.rs)
- [worker/src/execution/query.rs](worker/src/execution/query.rs)
- [worker/src/execution/join.rs](worker/src/execution/join.rs)

2. Updated runtime wiring:
- [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)
- [worker/src/services/query.rs](worker/src/services/query.rs)
- [worker/src/services/mod.rs](worker/src/services/mod.rs)
- [worker/src/main.rs](worker/src/main.rs)

## Exit Criteria
1. Query runtime logic no longer lives under services except temporary transition shims.
2. Execution module boundaries are explicit and compile-checked.
3. Roadmap intermission matrix has all mandatory criteria marked Done before signoff.
