# SILK_ROAD Discovery: INDEXING FOUNDATION

## Objective
Establish an evidence-backed path to introduce indexing-oriented query acceleration in Kionas, starting with predicate pruning and file skipping before introducing heavier index structures.

## System Path Under Analysis
1. Query parsing and logical planning
2. Physical planning and operator model
3. Server distributed DAG compilation and task payload metadata
4. Worker runtime scan execution path
5. Delta metadata and metastore extension points

## Executive Summary
Current runtime behavior always scans all parquet files under table staging prefixes and applies filtering after data load, which blocks predicate pruning and fast access paths. The most feasible first implementation direction is metadata-driven file skipping integrated into existing scan flow, with sidecar index catalog support deferred until metadata contracts are stabilized.

## Evidence Map

### 1) Physical TableScan Has No Index Strategy Envelope
- Evidence: [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs)
- Observed: PhysicalOperator::TableScan contains only relation metadata and no scan strategy, candidate file set, or predicate pushdown metadata.
- Conclusion: Planner currently has no type-safe path to express index-aware access behavior to runtime.

### 2) Physical Translation Emits TableScan Without Pruning Metadata
- Evidence: [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs)
- Observed: Translation constructs a scan-first pipeline without enriching scan operators with candidate file constraints.
- Conclusion: Predicate-aware scan narrowing must be introduced in translation or a subsequent planning enrichment pass.

### 3) Distributed Task Payload Is Extensible But Not Using Index Metadata
- Evidence: [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs)
- Observed: Stage task params transport JSON payload and metadata but do not include index eligibility, table statistics slices, or file pruning hints.
- Conclusion: Server-to-worker transport has extension room; scan hints can be introduced without replacing task envelope shape.

### 4) Worker Scan Path Performs Full Prefix Read
- Evidence: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)
- Observed: load_scan_batches lists all parquet files in prefix, sorts keys, reads every file, then filtering happens later in execution.
- Conclusion: File skipping can be implemented with low churn by inserting pruning logic before per-file read/decode loop.

### 5) Delta Access Exists But Snapshot Metadata Is Not Exploited For Scan Pruning
- Evidence: [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs)
- Observed: Delta open/create and commit flow exist, but runtime scan path does not consume per-file metadata to reduce read set.
- Conclusion: Delta metadata can be used as first-stage pruning signal before adding standalone index artifacts.

### 6) Metastore Has Table Metadata And Statistics Extension Points
- Evidence: [metastore/src/services/provider/mod.rs](metastore/src/services/provider/mod.rs)
- Evidence: [metastore/src/services/provider/postgres.rs](metastore/src/services/provider/postgres.rs)
- Evidence: [kionas/proto/metastore_service.proto](kionas/proto/metastore_service.proto)
- Observed: Existing provider/proto contract and storage backend can be extended to store index descriptors and future scan stats, though stats are currently limited.
- Conclusion: Sidecar index catalog is feasible as a follow-up phase once pruning MVP proves integration path.

## Root-Cause Decomposition

| Layer | Current State | Required Change |
|---|---|---|
| Physical operator model | TableScan has only relation identity | Add optional scan strategy/pruning metadata envelope |
| Translation | Scan emission is relation-only | Produce scan-level pruning intent from predicates |
| Task transport | Params map is generic but sparse | Include optional pruning/index metadata |
| Runtime scan | Full-file prefix scan then filter | Prune candidate files before read/decode |
| Metadata layer | Delta/Metastore metadata underused | Expose practical metadata for pruning decisions |

## Technical Unblock Dimensions

### A) Scan Strategy Model
- Define a minimal scan strategy contract for FOUNDATION: full scan vs metadata-pruned scan.
- metadata-driven pruning is the lowest-churn path to early wins; more complex index structures can be added later once the contract is proven.

### B) Predicate Eligibility Rules
- Restrict initial pruning to deterministic predicate shapes (equality and range on primitive columns).
- Defer unsupported predicate classes (complex expressions/UDF-shaped filters) to full scan fallback.

### C) Runtime Pruning Integration
- Insert pruning in worker scan loading before file read loop.
- Preserve deterministic ordering and fallback semantics when pruning metadata is absent.

### D) Metadata Freshness
- Define staleness/fallback behavior so stale metadata cannot silently produce wrong results.
- Fail safe to full scan when confidence signals are missing.

### E) Verification Envelope
- Add observability proving when pruning was attempted and applied.
- Validate result equivalence against full-scan behavior.

## Candidate Architecture Tradeoffs

### Candidate 1: Metadata-Only File Skipping (Recommended FOUNDATION Path)
- Description: Use table/Delta metadata and predicate eligibility rules to prune file read set before loading parquet bytes.
- Strengths: Lowest integration churn; leverages existing runtime insertion point.
- Risks: Metadata quality and freshness limits skip ratio.
- Decision: Recommended for first implementation phase.
- Discovery conclusions:
    - Metadata transport for FOUNDATION uses existing task params with explicit scan keys (`scan_mode`, optional `scan_pruning_hints_json`, optional `scan_delta_version_pin`) rather than proto changes.
    - Sidecar parquet index artifacts are not recommended for FOUNDATION because they add lifecycle and consistency burden that overlaps with Delta-native metadata.
    - Delta metadata is the primary source for pruning (table snapshot and file-level Add metadata), aligned with the project constraint that storage and lifecycle remain Delta-first.
    - Safety protocol is conservative by design: unsupported predicates or missing/invalid metadata must deterministically fall back to full scan.
    - Observability is mandatory: runtime must log requested scan mode and fallback reasons so pruning behavior is auditable before becoming default.

### Candidate 2: Sidecar Index Catalog
- Description: Introduce explicit index metadata objects attached to table metadata and consumed at runtime.
- Strengths: Cleaner long-term extensibility for multiple index types.
- Risks: Contract/schema additions and catalog lifecycle complexity.
- Decision: Defer to post-FOUNDATION phase.

### Candidate 3: Hybrid
- Description: Start metadata pruning and incrementally add sidecar catalog for high-value predicates.
- Strengths: Balanced migration path.
- Risks: Dual-path complexity early.
- Decision: Consider after FOUNDATION stabilization.

## Constraints and Risks
1. Metadata drift may reduce pruning correctness confidence.
- Mitigation: deterministic fallback to full scan when metadata checks fail.

2. Predicate class coverage is limited in first phase.
- Mitigation: explicit eligibility gate and conservative fallback.

3. Distributed stage behavior may mask pruning impact.
- Mitigation: emit stage-level diagnostics for candidate file count before/after pruning.

## Discovery Outcome
FOUNDATION discovery confirms that the fastest safe path is metadata-driven file skipping in worker scan loading with optional planner/task metadata enrichment. Sidecar index catalog work remains valuable but should follow once the pruning pipeline and correctness guardrails are proven.

Next iteration planning target: [roadmaps/SILK_ROAD/INDEXING/plans/plan-INDEXING-FOUNDATION.md](roadmaps/SILK_ROAD/INDEXING/plans/plan-INDEXING-FOUNDATION.md)
