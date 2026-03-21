# SILK_ROAD Discovery: CONSTRAINTS FOUNDATION

## Objective
Choose the next SILK_ROAD direction after INDEXING by comparing two options:
1. Enrich datatype handling (datetime/timestamp/date focus).
2. Follow the road to constraints.

This discovery provides a decision with explicit evidence, tradeoffs, and a safe next-step handoff.

## Closure Status
FOUNDATION implementation is complete and accepted.
- Signoff matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
- Roadmap status: [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md)

## System Path Under Analysis
1. Parser and query model extraction.
2. Server statement handling and task payload shaping.
3. Metastore metadata persistence and retrieval.
4. Worker transaction validation and execution behavior.
5. Planner validation and runtime capability gates.

## Executive Summary
Both roads are valuable. The recommended next road is CONSTRAINTS.

Why:
1. Lower implementation risk for first vertical slice.
2. Faster correctness value (starting with NOT NULL enforcement path).
3. Narrower blast radius and cleaner rollback behavior.
4. Preserves a clear follow-on lane for DATATYPES without blocking current query progress.

## Evidence Map

### Option A: DATATYPES (datetime/timestamp/date)
- Current type handling and coercion surface: [worker/src/transactions/ddl/create_table.rs](worker/src/transactions/ddl/create_table.rs)
- Delta/Arrow type mapping surface: [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs)
- Insert-time scalar and validation surface: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)
- Planner validation gate scope: [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs)

Observed risk:
1. Broader end-to-end dependency chain.
2. Higher chance of silent type/coercion bugs if done in a hurry.
3. More cross-cutting impact on joins, filters, aggregates, and future windows.

### Option B: CONSTRAINTS
- Constraint and schema payload shaping: [server/src/statement_handler/create_table.rs](server/src/statement_handler/create_table.rs)
- Query model surface for table constructs: [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs)
- Insert/transaction enforcement insertion point: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)

Observed opportunity:
1. Constraint enforcement is additive and auditable.
2. Easier phased rollout NOT NULL.
3. Strong correctness narrative with clear error taxonomy potential.


## Comparative Decision Table
| Dimension | DATATYPES | CONSTRAINTS |
|---|---|---|
| First-slice complexity | High | Medium |
| Quick-win potential | Medium-Low | High |
| Blast radius | Broad | Focused |
| Deferred-risk cost | High | Medium |
| Rollout safety | Medium | High |
| Recommended now | No | Yes |

## Decision
Proceed with CONSTRAINTS as the next SILK_ROAD road.

DATATYPES remains critical and should be pursued as the immediate follow-on discovery/implementation track after CONSTRAINTS FOUNDATION signoff.

## Scope Boundaries for Next Iteration
In scope:
1. Define constraint metadata lifecycle (create/store/read).
2. Enforce NOT NULL in mutation paths with explicit actionable errors.
3. The system will have only NOT NULL constraint. Modern ELT/ETL pipelines and tools like dbt can handle this.

Out of scope:
1. Full datatype-lattice redesign.
2. Broad temporal arithmetic semantics.
3. Mutation cost-model optimization.

## Handoff Targets
- Completed implementation plan: [roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md)
- Completion matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
