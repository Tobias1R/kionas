# ROADMAP_PARALLEL_PHASE1_MATRIX

## Phase 1: Distributed Control Plane Foundation

### Scope
Implement the distributed execution control plane for Phase 1:
- Task contract redesign using StagePartitionExecution metadata.
- Stage DAG scheduling with dependency-safe concurrent stage wave dispatch.
- Output destination mapping for downstream routing metadata.
- Validation and observability foundations required before Phase 2 data plane work.

Phase reference: [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md)

---

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| WS0.1 Proto definitions for StagePartitionExecution/OutputDestination/ExecutionModeHint | Done | [kionas/proto/worker_service.proto](kionas/proto/worker_service.proto) | Breaking contract implemented. |
| WS0.2 Task metadata model on server (typed stage carrier) | Done | [server/src/tasks/mod.rs](server/src/tasks/mod.rs) | StageTaskMetadata introduced and wired. |
| WS0.3 Server task dispatch emits typed stage metadata contract | Done | [server/src/tasks/mod.rs](server/src/tasks/mod.rs) | TaskRequest now maps typed stage metadata directly. |
| WS0.4 Serialization/mapping tests for task contract | Done | [server/src/tests/tasks_mod_tests.rs](server/src/tests/tasks_mod_tests.rs) | Includes typed metadata and query_run_id tests. |
| WS1.1 Stage group compilation with stage dependencies and partition metadata | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs) | Produces typed StageTaskGroup metadata. |
| WS1.2 RepartitionExec extraction module per plan technical approach | Done | [server/src/planner/stage_extractor.rs](server/src/planner/stage_extractor.rs), [server/src/planner/engine.rs](server/src/planner/engine.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs) | Dedicated extractor module implemented and wired into planner diagnostics and dispatch topology adaptation. |
| WS1.3 Stage extraction edge-case matrix (1-4 stage and broadcast shapes) | Done | [server/src/tests/planner_stage_extractor_tests.rs](server/src/tests/planner_stage_extractor_tests.rs), [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs) | Covers single-stage, linear multi-boundary, branching repartition paths, topology rewrite, and partition fanout routing behavior. |
| WS2.1 Topological stage-wave build with cycle rejection | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs) | Branching and cyclic DAG coverage present. |
| WS2.2 Concurrent stage-wave execution scheduler | Done | [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs) | Uses JoinSet for parallel stage-partition dispatch. |
| WS3.1 Output destination mapping for stage routing metadata | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs) | Destination metadata emitted per stage. |
| WS3.2 In-memory end-to-end stage execution harness | Done | [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs), [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) | Injectable in-memory partition executor seam added and validated for 1-, 2-, and 3-stage DAG chains. |
| WS3.3 Byte-identical validation matrix across query scenarios | Done | [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) | Deterministic in-memory baseline comparison added for 1-, 2-, and 3-stage DAG scenarios. |
| WS4.1 Stage context validation in server and worker | Done | [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs), [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs) | Typed stage context checks in place. |
| WS4.2 Observability metrics package (extraction, DAG width, validation) | Done | [server/src/planner/engine.rs](server/src/planner/engine.rs), [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs) | Extraction diagnostics, DAG width/fanout metrics, and validation telemetry are emitted in dispatch path; dashboarding remains follow-up ops work. |
| WS4.3 Runbooks and error-scenario integration matrix | Done | [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs), [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs) | Scheduler-level error matrix covers missing context, fanout mismatch, cycle rejection, timeout-style failures, and stage-context error propagation. |

---

## Mandatory Criteria (Phase 1 Signoff Gate)

| Criterion | Status | Evidence |
|---|---|---|
| Task model redesigned with StagePartitionExecution and ExecutionModeHint | Done | [kionas/proto/worker_service.proto](kionas/proto/worker_service.proto), [server/src/tasks/mod.rs](server/src/tasks/mod.rs) |
| Stage extraction at RepartitionExec boundaries implemented and tested | Done | [server/src/planner/stage_extractor.rs](server/src/planner/stage_extractor.rs), [server/src/tests/planner_stage_extractor_tests.rs](server/src/tests/planner_stage_extractor_tests.rs), [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs) |
| DAG scheduler implemented with dependency-safe async dispatch | Done | [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs) |
| Output destinations mapped for downstream stage partitions | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs) |
| In-memory execution harness validates full 1-3 stage queries | Done | [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs), [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) |
| Results are byte-identical to baseline execution | Done | [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) |
| Error cascade + tracing + metrics complete | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs), [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs), [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs) |
| Breaking changes documented and full-cluster upgrade path defined | Not Started | N/A |

Signoff rule: all mandatory criteria must be Done with concrete evidence.

---

## Signoff Decision

- Phase signoff: Pending
- Proceed to Phase 2: Blocked by incomplete mandatory criteria
- Blocking items:
  - Breaking changes documentation and full-cluster upgrade path

---

## Environment and Parameters

- KIONAS_STAGE_PARTITION_FANOUT (optional): controls hash-stage partition fanout when set to value > 1.
  - Usage: [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs)
- STAGE_PARTITION_FANOUT (optional fallback): same behavior as above.
  - Usage: [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs)
- distributed_dag_metrics_json (dispatch param): serialized DAG observability metrics attached to stage task groups.
  - Usage: [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs)

---

Matrix version: 2.0
Created: March 2026
Last updated: March 23, 2026 (mandatory error-cascade/tracing/metrics criterion completed)
Next review: After breaking-change upgrade-path evidence lands
