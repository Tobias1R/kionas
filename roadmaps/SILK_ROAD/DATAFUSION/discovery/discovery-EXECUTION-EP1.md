# Discovery: EXECUTION EP-1 Observability And Error Taxonomy Hardening

## Roadmap Link
- Parent roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)

## Planning Links
- EP1 plan: [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP1.md](../plans/plan-EXECUTION-EP1.md)

## Phase Scope
This discovery covers EP-1 only from the execution roadmap.

In scope:
- Objective 1: structured and stable execution error taxonomy.
- Objective 2: guaranteed context presence for critical failures.
- Objective 3: inspectable runtime decision diagnostics.

Out of scope:
- Implementation changes.
- Protocol redesign across client APIs.
- EP-2 and later optimization work.

## Discovery Method
1. Mapped error types and response envelopes across server, worker, shared kionas crate, and flight/proxy boundaries.
2. Traced context propagation from request ingress to worker execution and flight retrieval paths.
3. Audited explain, diagnostics, and runtime observability surfaces and compared them against EP-1 targets.
4. Identified blockers, risks, and decisions needed before plan authoring.

---

## Objective 1 Deep Dive: Structured And Stable Error Taxonomy

### Current State
1. Planner and query-model validation have typed error structures:
- [kionas/src/planner/error.rs](../../../kionas/src/planner/error.rs)
- [kionas/src/sql/query_model.rs](../../../kionas/src/sql/query_model.rs)

2. Server response envelopes map outcomes into business and infra categories:
- [server/src/services/warehouse_service_server.rs](../../../server/src/services/warehouse_service_server.rs)

3. Some high-value paths still rely on string matching for classification:
- [server/src/statement_handler/dml/insert.rs](../../../server/src/statement_handler/dml/insert.rs)

4. Worker response contract is message-centric for failure output:
- [kionas/proto/worker_service.proto](../../../kionas/proto/worker_service.proto)

5. Proxy and flight boundaries primarily preserve error text, not a unified code taxonomy:
- [flight_proxy/src/main.rs](../../../flight_proxy/src/main.rs)
- [worker/src/flight/server.rs](../../../worker/src/flight/server.rs)

### Observed Gaps
1. Taxonomy fragmentation exists between planner validation, server mapping, and runtime execution failures.
2. String-based classification paths are fragile and sensitive to wording changes.
3. Validation, execution, constraint, and infra failures are not consistently separated in all components.
4. Worker-to-server error semantics depend on message interpretation in several flows.

### Candidate Canonical Taxonomy Model
1. Category axis:
- Validation
- Execution
- Constraint
- Infra

2. Origin axis:
- Planner
- QueryModel
- TypeSystem
- WorkerExecution
- WorkerStorage
- Exchange
- MetadataResolve
- FlightProxy

3. Recoverability axis:
- Deterministic
- Transient
- Fatal

4. Remediation axis:
- UserAction
- AdminAction
- AutoRetry
- NoFix

### Decision Proposals For Plan Phase
1. Define canonical code format and naming conventions for all EP-1 touched surfaces.
2. Define compatibility strategy for current message-based worker error output.
3. Define migration rule from string matching to stable typed mapping in server handlers.
4. Define explicit ownership for taxonomy governance and versioning.

### Blockers And Challenges
1. Existing client-facing behavior may assume current business or infra prefixes.
2. Worker contract shape can limit immediate end-to-end typed propagation.
3. Partial rollout can create mixed semantics if not phase-gated tightly.
4. Multiple crates own error paths, so cross-crate alignment is required early.

### Evidence Snapshot
- [kionas/src/planner/error.rs](../../../kionas/src/planner/error.rs)
- [kionas/src/sql/query_model.rs](../../../kionas/src/sql/query_model.rs)
- [server/src/services/warehouse_service_server.rs](../../../server/src/services/warehouse_service_server.rs)
- [server/src/statement_handler/dml/insert.rs](../../../server/src/statement_handler/dml/insert.rs)
- [kionas/proto/worker_service.proto](../../../kionas/proto/worker_service.proto)
- [flight_proxy/src/main.rs](../../../flight_proxy/src/main.rs)

---

## Objective 2 Deep Dive: Ensure Context On Critical Failures

### Current Context Propagation Map
1. Query identity starts at request ingress and is propagated through dispatch metadata:
- [server/src/services/request_context.rs](../../../server/src/services/request_context.rs)
- [server/src/workers/mod.rs](../../../server/src/workers/mod.rs)

2. Stage and run identifiers are assigned during select orchestration and DAG decomposition:
- [server/src/statement_handler/query/select.rs](../../../server/src/statement_handler/query/select.rs)
- [server/src/statement_handler/shared/distributed_dag.rs](../../../server/src/statement_handler/shared/distributed_dag.rs)

3. Worker receives dispatch context and enriches task params for runtime use:
- [worker/src/services/worker_service_server.rs](../../../worker/src/services/worker_service_server.rs)
- [worker/src/authz.rs](../../../worker/src/authz.rs)

4. Runtime stage context is extracted and consumed by pipeline execution:
- [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
- [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)

5. Flight retrieval path reconstructs execution access context:
- [worker/src/flight/server.rs](../../../worker/src/flight/server.rs)

### Observed Gaps
1. Critical runtime failures do not always include full query, stage, and task context together.
2. Deep helper failures in execution and exchange paths can lose correlation fields.
3. Some error pathways surface partial context that is insufficient for fast triage.
4. Context consistency between worker runtime errors and server response envelopes is not guaranteed.

### Required EP-1 Context Contract For Planning
1. Minimum context for critical execution failures:
- query_id
- stage_id
- task_id
- partition index when relevant

2. Mandatory propagation checkpoints:
- server dispatch boundary
- worker task ingest boundary
- stage runtime context construction
- exchange read or write boundary
- flight retrieval boundary

3. Mandatory failure classes that must carry full context:
- runtime operator failures
- exchange artifact failures
- storage read or write failures
- dispatch and handoff failures

### Decision Proposals For Plan Phase
1. Define canonical context field names and source-of-truth ownership by hop.
2. Define where context enrichment happens versus where it must only be forwarded.
3. Define redaction and exposure policy for context in user-facing messages versus internal diagnostics.
4. Define acceptance tests for context completeness per failure class.

### Blockers And Challenges
1. Existing helper signatures are not uniformly context-aware.
2. Over-enrichment risks noisy errors and inconsistent formatting.
3. Server response shape may not carry all runtime fields without compatibility decisions.
4. Flight boundary context may require clear distinction between transport identifiers and diagnostic identifiers.

### Evidence Snapshot
- [server/src/services/request_context.rs](../../../server/src/services/request_context.rs)
- [server/src/workers/mod.rs](../../../server/src/workers/mod.rs)
- [server/src/statement_handler/query/select.rs](../../../server/src/statement_handler/query/select.rs)
- [server/src/statement_handler/shared/distributed_dag.rs](../../../server/src/statement_handler/shared/distributed_dag.rs)
- [worker/src/services/worker_service_server.rs](../../../worker/src/services/worker_service_server.rs)
- [worker/src/authz.rs](../../../worker/src/authz.rs)
- [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
- [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)
- [worker/src/flight/server.rs](../../../worker/src/flight/server.rs)

---

## Objective 3 Deep Dive: Inspectable Runtime Decision Diagnostics

### Current Diagnostics Surfaces
1. Server generates DataFusion planning artifacts for logical, optimized logical, and physical plans:
- [server/src/planner/engine.rs](../../../server/src/planner/engine.rs)

2. Shared explain infrastructure supports text and JSON explain outputs:
- [kionas/src/planner/explain.rs](../../../kionas/src/planner/explain.rs)

3. Worker runtime extracts execution shape and applies pipeline operators:
- [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
- [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)

4. Artifact metadata sidecars expose result-level metadata useful for post-run inspection:
- [worker/src/execution/artifacts.rs](../../../worker/src/execution/artifacts.rs)

### Observed Gaps
1. Not all available explain or diagnostic artifacts are surfaced in operator-queryable workflows.
2. Runtime decision points are not consistently captured in structured, searchable form.
3. Scan mode and fallback rationale are not uniformly exposed for operational debugging.
4. Operator-level diagnostics do not have a phase-defined minimal contract yet.

### Proposed EP-1 Observability Contract For Planning
1. Mandatory decision events to capture:
- scan mode chosen
- fallback reason
- stage dispatch boundary
- exchange input and output decisions
- final materialization decision

2. Mandatory dimensions on each critical event:
- query_id
- stage_id
- task_id
- operator family
- error category and origin

3. Explain integration expectations:
- define which explain artifacts remain internal versus externally queryable
- define retrieval path and retention expectations for debugging sessions

4. EP-1 acceptance evidence expectations:
- success-path diagnostic snapshot
- validation-failure diagnostic snapshot
- runtime-failure diagnostic snapshot
- infra-degradation diagnostic snapshot

### Decision Proposals For Plan Phase
1. Define minimal event schema and naming convention for EP-1.
2. Define baseline observability outputs required before EP-1 signoff.
3. Define balance between operator visibility and payload volume.
4. Define compatibility rules for current logs and future structured diagnostics.

### Blockers And Challenges
1. Existing log output is uneven across components and paths.
2. Diagnostic noise risk increases if events are added without taxonomy discipline.
3. End-to-end queryability requires aligned conventions across server, worker, and flight boundaries.
4. Phase scope must remain focused on EP-1 foundations and avoid EP-2 performance expansion.

### Evidence Snapshot
- [server/src/planner/engine.rs](../../../server/src/planner/engine.rs)
- [kionas/src/planner/explain.rs](../../../kionas/src/planner/explain.rs)
- [worker/src/execution/planner.rs](../../../worker/src/execution/planner.rs)
- [worker/src/execution/pipeline.rs](../../../worker/src/execution/pipeline.rs)
- [worker/src/execution/artifacts.rs](../../../worker/src/execution/artifacts.rs)
- [worker/src/storage/exchange.rs](../../../worker/src/storage/exchange.rs)

---

## Cross-Objective Dependencies
1. Objective 1 taxonomy decisions must precede objective 2 context contract finalization.
2. Objective 2 context contract must precede objective 3 event schema finalization.
3. Objective 3 acceptance evidence must map directly to taxonomy and context requirements from objectives 1 and 2.

## Risk Register
1. Mixed error semantics during rollout can reduce diagnostic trust.
2. Incomplete context propagation can mask root-cause location.
3. Overly broad observability scope can delay EP-1 plan signoff.
4. Compatibility constraints can force phased migration that needs strict matrix tracking.

## EP-1 Plan Readiness Outputs
This discovery produces the required inputs to author plan-EXECUTION-EP1:
1. Taxonomy model candidates and decision points.
2. Context propagation contract requirements and checkpoints.
3. Observability event contract requirements and acceptance evidence shape.
4. Blockers and risks requiring explicit mitigation tasks in the EP-1 implementation plan.

## Open Questions For Plan Authoring
1. Should worker error contracts be expanded in EP-1 or adapted at server boundary first?
2. Which diagnostic artifacts are mandatory user-queryable in EP-1 versus operator-only?
3. What is the minimum acceptable structured event set for EP-1 signoff without over-scope?

## Signoff Handshake To EP-1 Plan
Before plan creation, confirm:
1. Canonical taxonomy format and ownership.
2. Mandatory context fields for critical failures.
3. Mandatory observability event and evidence set.
4. Any compatibility constraints that must be marked as Deferred in the EP-1 matrix.
