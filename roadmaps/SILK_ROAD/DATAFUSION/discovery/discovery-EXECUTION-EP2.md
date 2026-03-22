# EP-2 Discovery: Scan Pruning Completion And Scan-Mode Truthfulness

## Scope
This discovery focuses only on EP-2 goals from ROADMAP_EXECUTION:
1. Complete metadata-driven pruning behavior for base scan paths.
2. Ensure telemetry distinguishes true pruning from full-scan fallback.
3. Validate safe degradation for stale or incomplete pruning metadata.

In scope:
- Server emission of scan hints and scan mode task params.
- Worker runtime interpretation of scan mode and pruning hints.
- Base scan object listing and metadata-pruning application path.
- Current tests that prove pruning and fallback semantics.
- Observability truthfulness gaps and closure criteria.

Out of scope:
- Join/aggregate runtime expansion.
- New protocol contracts.
- Code changes (discovery-only artifact).

## Key Evidence Reviewed
- worker/src/execution/pipeline.rs
- worker/src/execution/planner.rs
- worker/src/tests/execution_pipeline_tests.rs
- server/src/statement_handler/query/select.rs
- roadmaps/discover/execution_pipeline_discovery.md
- roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md

## Current Architecture Slice (EP-2 Relevant)

### Server-side hint production
Current behavior in select dispatch:
1. Server builds scan pruning hints JSON from filter operators.
2. Server marks scan_mode as metadata_pruned when hint payload says eligible=true, else full.
3. Server resolves a delta snapshot pin (scan_delta_version_pin), or emits sentinel 0 with warning if unavailable.
4. Server always injects scan metadata params into stage task params.

Observed implications:
- Hints and mode are explicit in task params.
- Eligibility and reason are present in hint payload.
- Sentinel pin path exists and is logged.

### Worker-side hint parsing and context
Current behavior in stage context derivation:
1. RuntimeScanMode supports full and metadata_pruned.
2. scan_pruning_hints_json and scan_delta_version_pin are parsed from task params.
3. pruning_eligible and pruning_reason are derived from hint JSON.
4. Missing or invalid hints convert to deterministic reasons:
   - missing_pruning_hints
   - invalid_pruning_hints_json
   - ineligible_or_missing_reason

Observed implications:
- Worker has enough state to decide pruning vs fallback and to explain why.

### Worker base scan path
Current behavior in load_scan_batches:
1. Base parquet keys are listed and sorted deterministically.
2. maybe_prune_scan_keys is called before reading parquet objects.
3. If pruning removes all keys, execution fails with explicit error.
4. If no source parquet exists, execution fails and higher-level empty-table fallback path handles this only in specific contexts.

Current behavior in maybe_prune_scan_keys:
1. Pruning only activates when mode=metadata_pruned and pruning_eligible=true.
2. Any hint parse issue, table prefix issue, pin mismatch, delta metadata load issue, or empty stats triggers fallback to original key set.
3. Cache hit/miss is handled with explicit logging.
4. Positive pruning emits retained/pruned file counts in logs.

Observed implications:
- Metadata pruning is already functionally active in worker for supported scenarios.
- Fallback is graceful and deterministic.

## Truthfulness Assessment

## Finding 1: Runtime behavior is ahead of one legacy log message
In load_scan_batches, one info log still says:
- metadata_pruned requested ... fallback to full scan until pruning phase is implemented

But in the same execution path, maybe_prune_scan_keys is invoked and can actively prune keys.

Impact:
- This is an observability truthfulness mismatch.
- Operators may incorrectly conclude pruning is disabled even when pruning occurred.

Severity:
- Medium (diagnostics correctness issue, not data correctness issue).

## Finding 2: Telemetry is mostly log-text based and not normalized event dimensions
Current pruning visibility is strong but string-log oriented.

Impact:
- Harder aggregation by reason/fallback category in operations dashboards.
- Harder to distinguish requested mode vs effective mode without log parsing.

Severity:
- Medium.

## Finding 3: Fallback reasons are broad but not yet grouped into explicit low-cardinality classes
Reasons are present, but a stable reason taxonomy matrix for EP-2 is not yet documented as a contract.

Impact:
- Potential drift in operational semantics across future changes.

Severity:
- Low to medium.

## Existing Test Coverage Snapshot
From worker pipeline tests:
1. pruning_falls_back_on_pin_mismatch
2. pruning_falls_back_when_stats_missing
3. pruning_removes_definitely_false_file

These prove:
- Safe fallback on stale pin mismatch.
- Safe fallback when stats are not usable.
- Actual key pruning when metadata predicate evaluation is definitive.

Coverage gap for EP-2 closure:
- No test that validates requested mode vs effective pruning outcome telemetry contract.
- No explicit test for delta log unavailable fallback telemetry category.
- No explicit test for invalid hints JSON fallback telemetry category.

## EP-2 Risk Register
1. Diagnostic trust risk:
- Mismatch between message text and actual pruning behavior can mislead incident triage.

2. Metrics blind spot:
- Without structured counters, proving pruning effectiveness is costly.

3. Sentinel pin ambiguity:
- scan_delta_version_pin=0 path is safe but can hide root cause frequency unless grouped telemetry is added.

4. Empty input edge coupling:
- Base-scan no-file behavior depends on broader pipeline fallback semantics; EP-2 should preserve this correctness boundary.

## Recommended EP-2 Implementation Plan Inputs

### Workstream A: Scan-mode truth model
1. Define explicit terms in code/docs/tests:
- requested_mode (from task params)
- effective_mode (full or metadata_pruned)
- fallback_reason (low-cardinality)
2. Ensure logs/events always report both requested and effective values.

### Workstream B: Fallback reason taxonomy
Create bounded fallback reason classes (example shape):
1. hints_missing
2. hints_invalid
3. predicate_ast_missing
4. delta_pin_mismatch
5. delta_version_unavailable
6. delta_stats_unavailable
7. source_prefix_invalid
8. table_prefix_invalid

### Workstream C: Telemetry normalization
1. Emit a structured pruning decision event (single canonical shape).
2. Include: query_id, stage_id, task_id, requested_mode, effective_mode, reason, delta_version_pin, cache_hit, total_files, retained_files.
3. Keep reason cardinality bounded.

### Workstream D: Test expansion for truthfulness
Add targeted tests for:
1. requested metadata_pruned with effective full due to each major fallback reason.
2. requested metadata_pruned with effective metadata_pruned and retained_files < total_files.
3. requested full with hints present but ignored path.

### Workstream E: Safety and edge-case verification
1. Preserve deterministic key ordering under pruning and fallback.
2. Verify empty-table behavior remains correct in no-parquet scenarios.
3. Verify stale pin and metadata fetch failures never break query correctness.

## EP-2 Completion Readiness Mapping
Against roadmap mandatory criteria:

1. Pruning flow implemented and verified:
- Partially satisfied now (worker has active pruning and baseline tests).
- Needs expanded targeted scenario coverage and explicit effective-mode assertions.

2. Telemetry indicates selected mode and fallback reasons:
- Partially satisfied now (log strings exist).
- Needs structured, low-cardinality truth model and removal of legacy contradictory message.

3. Stale metadata degrades safely:
- Mostly satisfied now (pin mismatch and stats-missing fallback proven).
- Needs broader stale/unavailable metadata fallback evidence.

4. Empty and non-empty table correctness:
- Partially satisfied now.
- Needs EP-2-specific evidence set tied to scan-mode permutations.

5. Quality gate evidence:
- Deferred to EP-2 implementation phase closure matrix.

## Dependencies and Assumptions
1. EP-1 signoff is complete before EP-2 implementation starts.
2. Task param contract remains compatible:
- scan_mode
- scan_pruning_hints_json
- scan_delta_version_pin
3. No protocol redesign required for EP-2.

## Discovery Output Summary
1. Worker already performs functional metadata pruning in supported cases.
2. Main EP-2 gap is scan-mode truthfulness and normalized telemetry, not pruning existence.
3. EP-2 should prioritize observability contract hardening and targeted fallback/test matrix completion.

## Proposed Next Artifact
Create EP-2 implementation plan at:
- roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP2.md

Plan should convert this discovery into:
1. Workstreamed tasks (truth model, telemetry, tests, closure evidence).
2. Mandatory criteria traceability table.
3. Command/evidence checklist aligned with ROADMAP_EXECUTION_EP2_MATRIX.md.
