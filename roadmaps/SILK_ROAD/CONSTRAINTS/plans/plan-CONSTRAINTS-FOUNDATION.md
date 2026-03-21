# Plan: CONSTRAINTS FOUNDATION

## Status
Completed and closed via signoff matrix.

Evidence:
1. [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
2. [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md)

## Objective
Establish the first implementation slice for constraints with correctness-first behavior and explicit fallback/error taxonomy.

## Implementation Steps

### Phase A: Metadata Contract
1. Normalize constraint representation across parser/server/metastore boundaries.
2. Define storage/retrieval shape for table and column constraints.
3. Keep backward compatibility for tables without constraints.
4. Prefer new modules instead extending existing ones to isolate constraint logic.


### Phase B: Enforcement Foundation
1. Enforce NOT NULL in insert/mutation paths.
2. Emit actionable violation errors with context.
3. Keep deterministic behavior under partial/invalid payloads.
4. Prefer new modules instead extending existing ones to isolate constraint logic.

Candidate files:
- [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs)
- [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs)

## Verification
1. Constraint metadata can be created, stored, and read without breaking existing tables.
2. NOT NULL violations are rejected with clear errors.
3. Existing valid inserts still succeed.
4. All mandatory matrix criteria are marked Done with evidence.

## Deliverables
1. Constraint metadata lifecycle definition.
2. NOT NULL enforcement implementation slice.
3. Matrix evidence updates for phase signoff.

## Completion Notes
1. Constraint metadata contract and validation are implemented in server/planner paths.
2. NOT NULL enforcement is active for INSERT, including implicit VALUES positional mapping.
3. Structured business error mapping for insert violations is in place.
