# EP-3A Discovery: Benchmark Readiness And Data Strategy (Workstream A Deep Dive)

## Purpose
This discovery decomposes EP-3 Workstream A into concrete readiness requirements.

Primary question:
- What do we really need to achieve meaningful memory and throughput benchmarks for EP-3?

Secondary questions:
- Do we need GB-scale data now?
- Do we need an ingestion-worker now?

## Inputs Reviewed
1. [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
2. [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP3.md](discovery-EXECUTION-EP3.md)
3. [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP3.md](../plans/plan-EXECUTION-EP3.md)
4. [scripts/generate_fake_data_sql.py](../../../../scripts/generate_fake_data_sql.py)
5. [scripts/generated_fake_data.sql](../../../../scripts/generated_fake_data.sql)
6. [scripts/client_workflow.sql](../../../../scripts/client_workflow.sql)
7. [worker/src/execution/pipeline.rs](../../../../worker/src/execution/pipeline.rs)
8. [worker/src/execution/join.rs](../../../../worker/src/execution/join.rs)
9. [worker/src/execution/artifacts.rs](../../../../worker/src/execution/artifacts.rs)

## Current State Summary
1. Benchmark workloads are defined at plan level but not operationalized as a reproducible runbook.
2. Deterministic synthetic data generation already exists via `scripts/generate_fake_data_sql.py`.
3. Existing generated SQL artifact is already non-trivial in size (`scripts/generated_fake_data.sql` is about 15 MB at last generation).
4. Query runtime hotspots for EP-3 remain scan/decode, join concat/index, normalization, and artifact serialization.
5. There is no dedicated ingestion-worker in current EP-3 scope.

## What We Actually Need Before Benchmarking

## Requirement 1: Benchmark protocol, not just queries
We need a fixed protocol that controls:
1. Dataset snapshot identity (seed + rows + schema name).
2. Warmup policy (cold run count, warm run count).
3. Measurement window (what starts/stops timer).
4. Repetition count per scenario.
5. Noise controls (same worker topology, no concurrent heavy jobs).

Without this, results are not phase-signoff quality.

## Requirement 2: Data tiers aligned to EP-3 objectives
We do not need one huge dataset immediately. We need data tiers:
1. Tier S (smoke): validates command and telemetry wiring.
2. Tier M (stress-lite): reveals variance and early memory pressure.
3. Tier L (stress): validates EP-3 acceptance under heavier load.

This tiering is mandatory for fast iteration plus realistic evidence.

## Requirement 3: Benchmark coverage matrix
Each profile must map to both intent and pressure point:
1. scan_only -> decode/list/read behavior.
2. filter_sort -> sort and normalization pressure.
3. join_heavy -> concat/index/fanout pressure.
4. aggregate_heavy -> reduction/grouping pressure.

## Requirement 4: Evidence schema for matrix signoff
Each run must produce consistent fields:
1. profile_id
2. dataset_tier
3. row_count and table_count
4. wall_time_ms
5. rows_processed
6. rows_per_second
7. artifact_size_bytes
8. run_id and timestamp

Optional but recommended:
1. stage_runtime_ms
2. batch_count
3. result_row_count

## Do We Need GBs Of Data Right Now?
Short answer: not immediately, but we do need a path to it.

Reasoning:
1. EP-3 is about optimization and predictable throughput, not pure max-scale hero runs.
2. Early optimization slices should be validated quickly on Tier M while preserving determinism.
3. Final EP-3 signoff should include at least one large-scale profile where data volume is high enough to surface join/materialization pressure.

Practical target strategy:
1. Start with Tier M using current synthetic generation flow.
2. Add Tier L once baseline harness is stable and repeatable.
3. Define Tier L by measured artifact and row-volume thresholds, not only file-size slogans.

## Is It Time For An Ingestion-Worker?
Short answer: not yet as a blocker for EP-3 start.

Why not now:
1. Existing deterministic SQL generation can bootstrap benchmark datasets.
2. EP-3 immediate blocker is benchmark protocol readiness, not ingestion feature completeness.
3. Building ingestion-worker now risks scope creep into a new subsystem before baseline evidence exists.

When ingestion-worker becomes justified (trigger conditions):
1. Data preparation time dominates benchmark cycle time.
2. SQL-based inserts become operationally unstable or too slow for Tier L repeatability.
3. We need repeatable bulk-load semantics that current scripts cannot provide.
4. Benchmark cadence is blocked by dataset provisioning overhead.

Decision gate:
- If two consecutive EP-3 iterations are blocked by data provisioning throughput, open an EP-3 optional hardening item or EP-3 intermission for ingestion-worker design.

## Data Provisioning Options (Near-Term)
1. Option A: Keep SQL generator only (current path)
- Pros: deterministic, already available, no architecture change.
- Cons: heavy SQL ingestion overhead at larger tiers.

2. Option B: Pre-generated staged artifacts plus metadata registration
- Pros: faster repeated benchmark cycles.
- Cons: requires curation and lifecycle discipline.

3. Option C: New ingestion-worker (deferred)
- Pros: scalable bulk ingestion pipeline.
- Cons: high scope and validation cost; not required to start EP-3.

Recommended now:
- Start with Option A + controlled tiering.
- Re-evaluate after baseline and first optimization slice.

## Proposed EP-3A Readiness Milestones
1. RM1: Benchmark protocol locked (runbook + evidence schema).
2. RM2: Tier S and Tier M datasets reproducibly provisioned from deterministic seed.
3. RM3: Baseline results captured for all four EP-3 profiles.
4. RM4: Decision checkpoint for Tier L scale-up and ingestion-worker necessity.

## Benchmark Readiness Acceptance Checklist
1. All profiles run with fixed dataset identity and parameters.
2. Each profile has at least 3 repeated runs.
3. Evidence table includes required fields for every run.
4. Baseline results are stable enough to compare post-change deltas.
5. Tier L go/no-go decision is recorded with rationale.

## Risks And Mitigations
1. Risk: benchmark invalidity due to environment drift.
Mitigation: fixed run protocol and repeated runs.

2. Risk: insufficient data pressure hides memory issues.
Mitigation: enforced tier progression with explicit Tier L gate.

3. Risk: over-investing in ingestion-worker too early.
Mitigation: trigger-based decision gate, not assumption-based scope expansion.

## Workstream A Refinement Back Into Plan
Workstream A should be split into two tracks:
1. A1 Protocol and evidence contract.
2. A2 Dataset tiering and provisioning readiness.

A2 outputs must include:
1. Tier definitions and thresholds.
2. Provisioning procedure per tier.
3. Tier L escalation gate with ingestion-worker trigger criteria.

## Conclusion
1. We can start EP-3 benchmarks without immediately building an ingestion-worker.
2. We should not delay EP-3 waiting for GB-scale ingestion infrastructure.
3. We do need a formal benchmark protocol plus tiered data strategy to make results credible.
4. Ingestion-worker should be considered only if data provisioning repeatedly blocks EP-3 execution cadence.
