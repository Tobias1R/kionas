# Plan: EXECUTION EP-1

## Goal
Deliver EP-1 Observability And Error Taxonomy Hardening for the execution pipeline by establishing a stable error taxonomy, enforcing critical-failure context completeness, and introducing a minimal inspectable diagnostics contract required for phase signoff.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP1.md](../discovery/discovery-EXECUTION-EP1.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](../../../ROADMAP_EXECUTION_EP1_MATRIX.md)

## Scope Boundaries
In scope:
1. Error taxonomy normalization for EP-1-covered validation, execution, constraint, and infra surfaces.
2. Context propagation hardening for critical execution failures.
3. Minimal runtime observability and diagnostics contract required by EP-1.
4. Evidence production for mandatory EP-1 criteria and matrix signoff.

Out of scope:
1. EP-2 and later optimization work.
2. Broad protocol redesign outside EP-1 compatibility needs.
3. Non-critical telemetry expansion beyond EP-1 minimum contract.

## Guardrails
1. Preserve worker execution contract boundaries while introducing compatible taxonomy and context improvements.
2. Keep rollout incremental and reversible at boundary adapters.
3. Avoid mixed semantic states at public response boundaries.
4. Keep diagnostics actionable without creating excessive noise.
5. Treat matrix evidence as a strict closure gate.

## Workstreams

### Workstream A: Contract Lock And Governance
Objectives:
1. Lock canonical taxonomy dimensions and naming conventions for EP-1.
2. Lock critical-failure context contract and field names.
3. Lock minimal decision-event schema for observability.
4. Lock compatibility strategy for message-based boundary paths.

Tasks:
1. Define taxonomy dimensions and code naming standard:
- category: validation, execution, constraint, infra
- origin: planner, query model, type system, worker execution, worker storage, exchange, metadata resolve, flight proxy
- recoverability: deterministic, transient, fatal
- remediation: user action, admin action, auto retry, no fix
2. Define compatibility policy:
- where adapter mappings are allowed
- where canonical codes are mandatory
3. Define mandatory context fields for critical failures:
- query_id
- stage_id
- task_id
- partition index when applicable
4. Define minimal EP-1 event schema:
- event name
- required dimensions
- emission boundary
- consumer visibility
5. Assign owners and review process for taxonomy and observability governance.

Deliverables:
1. Contract decisions section completed in this plan.
2. EP-1 compatibility policy documented and approved.
3. Context contract table and event schema table ready for implementation.

### Workstream B: Objective 1 Error Taxonomy Normalization
Objectives:
1. Remove fragile classification behavior in EP-1 scope.
2. Ensure consistent error semantics across server and worker boundaries.
3. Ensure public envelope mapping remains deterministic.

Primary surfaces:
1. [kionas/src/planner/error.rs](../../../kionas/src/planner/error.rs)    
2. [kionas/src/sql/query_model.rs](../../../kionas/src/sql/query_model.rs)
3. [server/src/services/warehouse_service_server.rs](../../../server/src/services/warehouse_service_server.rs)
4. [server/src/statement_handler/dml/insert.rs](../../../server/src/statement_handler/dml/insert.rs)
5. [server/src/statement_handler/query/select.rs](../../../server/src/statement_handler/query/select.rs)
6. [worker/src/services/worker_service_server.rs](../../../worker/src/services/worker_service_server.rs)
7. [kionas/proto/worker_service.proto](../../../kionas/proto/worker_service.proto)
8. [flight_proxy/src/main.rs](../../../flight_proxy/src/main.rs)

Tasks:
1. Build taxonomy inventory map by source file and emission boundary.
2. Define canonical code catalog for EP-1 in-scope failures.
3. Replace or isolate ad-hoc string classification behind deterministic mapping adapters.
4. Normalize server response mapping so category semantics are unambiguous.
5. Define migration notes and temporary compatibility behavior for message-only boundaries.
6. Define regression test coverage targets for taxonomy consistency.

Deliverables:
1. Canonical error catalog and mapping table.
2. Compatibility adapter plan for message-centric paths.
3. Taxonomy consistency test checklist.

### Workstream C: Objective 2 Context Propagation Hardening
Objectives:
1. Guarantee required context fields on critical failure surfaces.
2. Prevent context loss through helper chains and boundary conversions.
3. Align internal diagnostics and user-facing output policies.

Primary surfaces:
1. [server/src/services/request_context.rs](../../../server/src/services/request_context.rs)
2. [server/src/workers/mod.rs](../../../server/src/workers/mod.rs)
3. [server/src/statement_handler/query/select.rs](../../../server/src/statement_handler/query/select.rs)
4. [server/src/statement_handler/shared/distributed_dag.rs](../../../server/src/statement_handler/shared/distributed_dag.rs)
5. [worker/src/services/worker_service_server.rs](../../../worker/src/services/worker_service_server.rs)
6. [worker/src/authz.rs](../../../worker/src/authz.rs)
7. [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
8. [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)
9. [worker/src/storage/exchange.rs](../../../worker/src/storage/exchange.rs)
10. [worker/src/flight/server.rs](../../../worker/src/flight/server.rs)

Tasks:
1. Create hop-by-hop context propagation matrix:
- source field
- boundary
- required vs optional
- failure behavior when absent
2. Define mandatory context checkpoints:
- dispatch
- worker ingest
- stage context creation
- exchange read and write
- flight retrieval
3. Define context enrichment policy:
- where context is attached
- where context is only forwarded
4. Define redaction policy between internal diagnostics and user responses.
5. Define context completeness test scenarios by failure class:
- runtime operator failure
- exchange artifact failure
- storage io failure
- dispatch and handoff failure

Deliverables:
1. Context propagation contract and checkpoint checklist.
2. Context completeness acceptance tests plan.
3. User-facing versus internal context policy notes.

### Workstream D: Objective 3 Runtime Decision Diagnostics Contract
Objectives:
1. Make critical execution decisions inspectable.
2. Ensure decision diagnostics carry canonical dimensions.
3. Ensure explain and runtime diagnostics are retrievable for EP-1 signoff evidence.

Primary surfaces:
1. [server/src/planner/engine.rs](../../../server/src/planner/engine.rs)
2. [kionas/src/planner/explain.rs](../../../kionas/src/planner/explain.rs)
3. [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
4. [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)
5. [worker/src/execution/artifacts.rs](../../../worker/src/execution/artifacts.rs)
6. [worker/src/storage/exchange.rs](../../../worker/src/storage/exchange.rs)

Tasks:
1. Define minimal EP-1 decision-event catalog:
- scan mode chosen
- fallback reason
- stage dispatch boundary
- exchange input and output decision
- final materialization decision
2. Define required event dimensions:
- query_id
- stage_id
- task_id
- operator family
- category and origin
3. Define explain integration policy:
- internal only vs externally queryable artifacts
- retrieval and retention boundaries for EP-1
4. Define acceptance evidence pack:
- success-path diagnostic snapshot
- validation-failure snapshot
- runtime-failure snapshot
- infra-degradation snapshot
5. Define noise-control limits and event naming governance.

Deliverables:
1. EP-1 event schema table.
2. Explain and diagnostics visibility policy.
3. Acceptance evidence checklist for matrix signoff.

### Workstream E: Verification, Quality Gates, And Signoff Packaging
Objectives:
1. Convert all EP-1 mandatory criteria into explicit evidence tasks.
2. Ensure verification is reproducible and auditable.
3. Prepare signoff-ready matrix updates.

Tasks:
1. Build criteria-to-evidence traceability table for EP-1 mandatory criteria.
2. Define automated verification command set:
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
- cargo check
3. Define targeted test runs for taxonomy, context completeness, and diagnostics contract.
4. Define manual validation checklist for cross-service boundary scenarios.
5. Prepare matrix update checklist for [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](../../../ROADMAP_EXECUTION_EP1_MATRIX.md):
- Done evidence references
- Deferred rationale
- blocking item review
6. Record final EP-1 signoff decision inputs.

Deliverables:
1. Verification pack and command log template.
2. Matrix-ready evidence references per mandatory criterion.
3. EP-2 readiness handoff notes.

## Sequence And Dependencies
1. Workstream A is a hard prerequisite for all other workstreams.
2. Workstream B starts after Workstream A and establishes taxonomy primitives.
3. Workstream C starts after Workstream A and can run in parallel with late Workstream B tasks.
4. Workstream D starts after Workstream A and runs in parallel with Workstream C.
5. Workstream E closes the phase after B, C, and D outputs are complete.

## Milestones
1. M1: Contract lock complete.
2. M2: Taxonomy normalization design complete.
3. M3: Context propagation contract complete.
4. M4: Diagnostics contract complete.
5. M5: Verification pack complete and matrix updated for signoff.

## Acceptance Criteria
1. Canonical error taxonomy is defined and consistently applied for EP-1 in-scope surfaces.
2. Critical failure paths include required context fields at defined checkpoints.
3. Mandatory runtime decision events are defined with required dimensions and evidence outputs.
4. EP-1 evidence pack covers success, validation failure, runtime failure, and infra degradation scenarios.
5. Quality gates pass and matrix mandatory criteria can be marked Done with concrete evidence.

## Risks And Mitigations
1. Risk: mixed taxonomy semantics during migration.
Mitigation: boundary adapters with explicit sunset criteria.
2. Risk: context loss through helper layers.
Mitigation: checkpoint-based completeness tests and failure contracts.
3. Risk: diagnostics noise and low signal.
Mitigation: minimal event schema and governance constraints.
4. Risk: scope creep into EP-2.
Mitigation: strict in-scope guardrails and deferred backlog tracking.

## Deferred Candidate Backlog
1. Deep telemetry and metrics expansion beyond EP-1 minimal contract.
2. Broad transport and protocol redesign beyond compatibility adapters.
3. Additional optional hardening items not required for EP-1 mandatory signoff.

## E2E Tests
- On the end of this plan provide expectated test scenarios(SQL queries) if any.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP1.md](../discovery/discovery-EXECUTION-EP1.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix: [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](../../../ROADMAP_EXECUTION_EP1_MATRIX.md)

## Execution Record

This section captures Workstreams A-E outputs for EP-1 execution.

### Workstream A Output: Contract Lock And Governance

#### Canonical Taxonomy Dimensions And Naming Standard

Canonical dimensions:
1. category: validation | execution | constraint | infra
2. origin: planner | query_model | type_system | worker_execution | worker_storage | exchange | metadata_resolve | flight_proxy
3. recoverability: deterministic | transient | fatal
4. remediation: user_action | admin_action | auto_retry | no_fix

Canonical code format:
- `<CATEGORY>_<ORIGIN>_<REASON>`
- category and origin are uppercase normalized tokens.
- reason is uppercase snake case, stable across releases.

Public envelope format:
- `RESULT|<CATEGORY>|<CODE>|<MESSAGE>`

#### EP-1 Compatibility Policy

| Boundary | Canonical Code Required | Adapter Allowed | Sunset Rule |
|---|---|---|---|
| Planner/query-model internal errors | Yes | No | N/A |
| Server statement outcome envelope | Yes | No | N/A |
| Worker gRPC message-only error text | Yes at server mapping boundary | Yes | Remove adapter after proto-level typed code fields are introduced |
| Flight/proxy text-only transport errors | Yes at proxy/server edge mapping | Yes | Remove adapter when structured transport error fields are available |

Adapter rules:
1. Adapters are only allowed on message-centric boundaries where typed code is unavailable.
2. Adapters must map to one canonical code deterministically.
3. Fallback catch-all code is allowed only as `INFRA_GENERIC` and must include raw source message in internal diagnostics.

#### Mandatory Critical-Failure Context Contract

| Field | Type | Required | Source Of Truth | Required At Boundaries |
|---|---|---|---|---|
| query_id | string | Yes | request context ingress | dispatch, worker ingest, stage context, exchange, flight retrieval |
| stage_id | u32/string parseable | Yes for staged execution | distributed dag task params | dispatch, worker ingest, stage context, exchange |
| task_id | string | Yes | task envelope | dispatch, worker ingest, stage context, exchange, flight retrieval |
| partition_index | u32/string parseable | Required when partitioned stage | task params/stage context | worker ingest, stage context, exchange |

Failure behavior when absent:
1. If query_id is absent at critical runtime boundaries, classify as `INFRA_WORKER_EXECUTION_CONTEXT_MISSING`.
2. If stage_id is absent for staged operators, classify as `EXECUTION_WORKER_EXECUTION_STAGE_CONTEXT_MISSING`.
3. If task_id is absent, classify as `INFRA_WORKER_EXECUTION_TASK_CONTEXT_MISSING`.
4. If partition_index is required and absent, classify as `EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING`.

#### Minimal EP-1 Decision-Event Schema

| Event Name | Required Dimensions | Emission Boundary | Consumer Visibility |
|---|---|---|---|
| execution.scan_mode_chosen | query_id, stage_id, task_id, operator_family, category, origin, scan_mode | worker runtime planner/pipeline | internal diagnostics |
| execution.scan_fallback_reason | query_id, stage_id, task_id, operator_family, category, origin, reason | worker runtime planner/pipeline | internal diagnostics |
| execution.stage_dispatch_boundary | query_id, stage_id, task_id, category, origin, partition_count | server dispatch to worker | internal diagnostics |
| execution.exchange_io_decision | query_id, stage_id, task_id, partition_index, operator_family, category, origin, direction | worker exchange read/write | internal diagnostics |
| execution.materialization_decision | query_id, stage_id, task_id, operator_family, category, origin, output_format | worker final materialization | internal diagnostics |

Noise control rules:
1. Emit each decision event once per task and decision point.
2. Do not emit row-level diagnostics in EP-1.
3. Keep dimensions bounded cardinality except query_id and task_id.

Governance:
1. Taxonomy and event schema ownership: server+worker maintainers.
2. Review gate: any new category/origin token requires roadmap-linked review note.

### Workstream B Output: Error Taxonomy Normalization

#### Taxonomy Inventory Map

| Source File | Current Surface | Emission Boundary | EP-1 Action |
|---|---|---|---|
| [kionas/src/planner/error.rs](../../../kionas/src/planner/error.rs) | Typed planner error enum | shared crate internal | map each variant to canonical category/origin |
| [kionas/src/sql/query_model.rs](../../../kionas/src/sql/query_model.rs) | validation string codes | shared crate internal | align codes to canonical format |
| [server/src/services/warehouse_service_server.rs](../../../server/src/services/warehouse_service_server.rs) | response envelope mapping | public server response | enforce deterministic category mapping |
| [server/src/statement_handler/dml/insert.rs](../../../server/src/statement_handler/dml/insert.rs) | substring-based classification | server internal -> public envelope | isolate string matching behind deterministic adapter |
| [server/src/statement_handler/query/select.rs](../../../server/src/statement_handler/query/select.rs) | select dispatch and errors | server dispatch/response | require canonical code pass-through |
| [worker/src/services/worker_service_server.rs](../../../worker/src/services/worker_service_server.rs) | worker execute task errors | worker response | attach canonical context and stable code |
| [kionas/proto/worker_service.proto](../../../kionas/proto/worker_service.proto) | message-centric error transport | server-worker contract | keep compatibility adapter in EP-1 |
| [flight_proxy/src/main.rs](../../../flight_proxy/src/main.rs) | transport error wrapping | flight edge | apply canonical mapping at edge |

#### Canonical EP-1 Error Catalog

| Category | Origin | Code | Recoverability | Remediation |
|---|---|---|---|---|
| validation | planner | VALIDATION_PLANNER_UNSUPPORTED_QUERY_SHAPE | deterministic | user_action |
| validation | query_model | VALIDATION_QUERY_MODEL_INVALID_PROJECTION | deterministic | user_action |
| validation | type_system | VALIDATION_TYPE_SYSTEM_COERCION_FAILED | deterministic | user_action |
| constraint | worker_execution | CONSTRAINT_WORKER_EXECUTION_NOT_NULL_VIOLATION | deterministic | user_action |
| execution | worker_execution | EXECUTION_WORKER_EXECUTION_OPERATOR_FAILED | deterministic | admin_action |
| execution | exchange | EXECUTION_EXCHANGE_PARTITION_MISSING | deterministic | admin_action |
| execution | exchange | EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING | deterministic | admin_action |
| infra | worker_storage | INFRA_WORKER_STORAGE_IO_FAILED | transient | auto_retry |
| infra | metadata_resolve | INFRA_METADATA_RESOLVE_TABLE_NOT_FOUND | deterministic | user_action |
| infra | flight_proxy | INFRA_FLIGHT_PROXY_STREAM_FAILED | transient | auto_retry |
| infra | worker_execution | INFRA_WORKER_EXECUTION_CONTEXT_MISSING | fatal | admin_action |
| infra | worker_execution | INFRA_WORKER_EXECUTION_TASK_CONTEXT_MISSING | fatal | admin_action |
| infra | worker_execution | INFRA_GENERIC | transient | admin_action |

#### Compatibility Adapter Plan For Message-Centric Paths

1. Continue mapping message-only worker/proxy errors to canonical codes at server/edge adapters.
2. Keep a deterministic lookup table for known error fragments.
3. Route unknown message patterns to `INFRA_GENERIC` and persist raw detail in internal logs only.
4. Track all adapter hits for eventual proto contract hardening in a later phase.

#### Taxonomy Consistency Test Checklist

1. Every known insert dispatch error fragment maps to a single canonical code.
2. `RESULT` envelope category matches code prefix category.
3. Validation paths do not downgrade to infra categories.
4. Message-only worker failures map deterministically to canonical fallback codes.

### Workstream C Output: Context Propagation Hardening

#### Hop-By-Hop Context Propagation Matrix

| Source Field | Boundary | Required | Behavior If Missing |
|---|---|---|---|
| query_id | request ingress -> server dispatch | Yes | fail with canonical infra context-missing code |
| query_id | server dispatch -> worker ingest | Yes | fail at worker ingest |
| stage_id | distributed dag -> worker ingest | Yes for staged query tasks | fail stage context creation |
| task_id | task envelope -> worker ingest | Yes | reject request |
| partition_index | stage context -> exchange io | required for partitioned exchange | fail exchange decision and mark execution error |
| query_id/task_id | flight ticket/metadata -> flight retrieval | Yes | reject retrieval as context incomplete |

#### Mandatory Context Checkpoints

1. Dispatch boundary: verify query_id, stage_id, task_id before sending task.
2. Worker ingest: verify task_id and query_id; stage_id/partition_index when task is staged.
3. Stage context creation: fail-fast if stage metadata is not parseable.
4. Exchange read/write: require stage_id and partition_index on exchange operations.
5. Flight retrieval: require query/session scope + task_id before artifact lookup.

#### Context Enrichment Policy

1. Attach context at first creation boundary: request ingress and DAG stage compilation.
2. Forward context unchanged across helper layers unless enrichment is explicitly required.
3. Enrichment allowed only at dispatch, worker ingest, and flight retrieval boundaries.

#### Redaction Policy (Internal vs User-Facing)

1. User-facing envelope includes category, code, and concise actionable message.
2. Internal diagnostics include raw error source and complete context dimensions.
3. Sensitive metadata or path internals are excluded from user-facing messages.

#### Context Completeness Acceptance Tests Plan

1. Runtime operator failure includes query_id, stage_id, task_id.
2. Exchange artifact failure includes query_id, stage_id, task_id, partition_index.
3. Storage io failure includes query_id, stage_id, task_id and infra category.
4. Dispatch/handoff failure includes query_id and task_id before worker execution.

### Workstream D Output: Runtime Decision Diagnostics Contract

#### Event Schema Table (EP-1)

| Event | Required Dimensions | Trigger |
|---|---|---|
| execution.scan_mode_chosen | query_id, stage_id, task_id, operator_family, category, origin, scan_mode | runtime plan stage initialization |
| execution.scan_fallback_reason | query_id, stage_id, task_id, operator_family, category, origin, reason | fallback branch from pruning/optimized path |
| execution.stage_dispatch_boundary | query_id, stage_id, task_id, category, origin, partition_count | dispatch task groups to worker |
| execution.exchange_io_decision | query_id, stage_id, task_id, partition_index, operator_family, category, origin, direction | exchange read or write |
| execution.materialization_decision | query_id, stage_id, task_id, operator_family, category, origin, output_format | final artifact materialization |

#### Explain And Diagnostics Visibility Policy

1. Explain plan text/json remains queryable through existing explain surfaces.
2. EP-1 decision events are internal diagnostics (logs/structured events), not public API payload fields.
3. Retrieval boundary for EP-1 evidence is command logs plus service logs for targeted sessions.

#### Acceptance Evidence Checklist

1. Success-path diagnostic snapshot includes scan decision + materialization decision.
2. Validation-failure snapshot includes canonical validation code and query_id.
3. Runtime-failure snapshot includes canonical execution code with full context fields.
4. Infra-degradation snapshot includes canonical infra code and remediation class.

### Workstream E Output: Verification, Quality Gates, And Matrix Packaging

#### Criteria-To-Evidence Traceability

| Mandatory Criterion | Evidence Target |
|---|---|
| Error taxonomy standardized | canonical catalog + mapping inventory in this plan; server/worker mapping tests |
| Critical failures include required context | context propagation matrix + context completeness tests |
| Runtime decisions observable and testable | event schema table + diagnostic snapshot checklist |
| Regression coverage for classification/context | targeted tests checklist and command log outputs |
| Quality gates pass | `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo check` logs |

#### Automated Verification Command Set

1. `cargo fmt --all`
2. `cargo clippy --all-targets --all-features -- -D warnings`
3. `cargo check`

Command log template:

| Command | Status | Evidence |
|---|---|---|
| cargo fmt --all | Pending | attach terminal log reference |
| cargo clippy --all-targets --all-features -- -D warnings | Pending | attach terminal log reference |
| cargo check | Pending | attach terminal log reference |

#### Manual Validation Checklist

1. Server validation failure propagates canonical validation code.
2. Worker runtime failure propagates canonical execution code and full context.
3. Exchange missing partition error includes partition_index and stage_id.
4. Flight retrieval failure preserves task context and canonical infra mapping.

#### Matrix Update Checklist

1. Mark mandatory criteria as Done only when code + tests + gates are complete.
2. Include concrete evidence references for every Done item.
3. Mark optional hardening as Deferred with non-blocking rationale when not implemented.
4. Record signoff decision and blocking items explicitly.

#### EP-2 Readiness Handoff Notes

1. EP-2 can start only after EP-1 mandatory criteria are marked Done in matrix.
2. EP-2 must reuse EP-1 canonical taxonomy/event dimensions for scan pruning telemetry.

## E2E Expected Test Scenarios (SQL)

1. Success path with scan decision and materialization:
	- `SELECT id, name FROM default.users WHERE id > 10 ORDER BY id LIMIT 5;`
2. Validation failure path:
	- `SELECT missing_col FROM default.users;`
3. Runtime failure path (requires injected faulty exchange artifact for target stage):
	- `SELECT u.id FROM default.users u JOIN default.orders o ON u.id = o.user_id;`
4. Infra degradation path (requires temporary storage/network fault injection):
	- `SELECT COUNT(*) FROM default.large_events;`
