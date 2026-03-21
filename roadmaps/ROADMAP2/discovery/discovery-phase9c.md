# Phase 9c Discovery: BETWEEN/IN Predicates

## Overview
Phase 9c expands Phase 9b's type coercion foundation to support BETWEEN and IN predicates. These operators are foundational for range queries and membership tests, both critical for query expressiveness.

**Current state**: BETWEEN and IN appear in rejected predicate list; parser/planner support present but gated.
**Target state**: Full vertical support for BETWEEN and IN predicates across parser → planner → worker → flight with type safety.

## Scope

### In Scope
1. **BETWEEN predicate**: `column BETWEEN lower AND upper`
   - Single column queries: `SELECT * FROM t WHERE x BETWEEN 1 AND 10`
   - Combined with filters: `WHERE x > 0 AND x BETWEEN 1 AND 10`
   - NULL semantics: NULL operand → NULL result
   - Type coercion: strict matching with column type (int, bool, string, timestamp, decimal)

2. **IN predicate**: `column IN (val1, val2, ..., valN)`
   - Single column queries: `SELECT * FROM t WHERE id IN (1, 2, 3)`
   - Combined with filters: `WHERE status = 'active' AND id IN (10, 11, 12)`
   - NULL handling: NULL in list → NULL; NULL operand → NULL result
   - Type coercion: all values must match column type exactly (Strict policy)

3. **Type safety**
   - Enforce DecimalCoercionPolicy::Strict for all operands
   - Validate BETWEEN bounds (lower ≤ upper) at planning time if constants
   - Validate IN list homogeneity (all same type)
   - Preserve Phase 9b type validation infrastructure

4. **Integration**
   - Work with Phase 9b's schema_metadata threading
   - Reuse error taxonomy (MissingColumn, TypeMismatch, UnsupportedLiteral, plus new)
   - End-to-end tests: single-table, JOIN, GROUP BY, ORDER BY contexts

### Out of Scope
1. Implicit type coercions (reserved for Phase 10+)
2. Subqueries in IN clause (e.g., `WHERE id IN (SELECT id FROM ...)`) — Phase 10
3. Function calls in BETWEEN/IN bounds (deferred)
4. OR combinations with BETWEEN/IN (constraint: Phase 9 uses AND-only for simplicity)
5. Nested BETWEEN/IN (not critical for Phase 9c)

## Technical Foundation

### Parser Status
- BETWEEN and IN already recognized by parser (in restricted predicate list)
- Tokens available: BETWEEN, IN, AND
- Current state: parsed but rejected in predicate validation

### Logical Plan
- BETWEEN represented as: `column >= lower AND column <= upper` (internal representation)
- IN represented as: sequence of equality checks combined with OR (currently blocked)
- Alternative: new logical operators `LogicalBetween` and `LogicalIn` (better for semantics)

### Physical Plan
- PhysicalExpr enum needs extension: `Between { column, lower, upper }` and `In { column, values }`
- OR alternative: keep current AND/OR representation but unblock validation gates
- Type metadata already carries through schema_metadata field (Phase 9b)

### Worker Execution
- Filter pipeline (stages 1-4) must handle BETWEEN and IN operators
- Stage 3: apply filter mask with BETWEEN/IN evaluation logic
- Type validation: reuse Phase 9b's validate_type_compatibility() with list handling for IN

### Error Handling
- New scenarios:
  - **InvalidBetweenBounds**: lower > upper (detected at planning time if constants)
  - **InListTypeHomogeneity**: operands in IN list have different types
  - **BetweenEmptySet**: literal optimization: `BETWEEN 1 AND 0` → always false
- Extend format_type_error() and format_planner_type_error() with new scenarios

## Blockers and Dependencies

### Dependencies on Phase 9b
- ✅ Schema metadata threading (PhysicalPlan.schema_metadata) — COMPLETE
- ✅ Type validation infrastructure (validate_type_compatibility) — COMPLETE
- ✅ Error taxonomy (MissingColumn, TypeMismatch) — COMPLETE
- ✅ Worker runtime type checking (apply_filter_pipeline) — COMPLETE

### No Blockers
- Parser already recognizes BETWEEN/IN
- Type system frozen (DecimalCoercionPolicy::Strict, TimezonePolicy::UTC)
- Physical plan can be extended without disruption

## Implementation Strategy

### Phase 9c Step 1: Planner Support (Est. 4-6 hours)
1. Extend PhysicalExpr enum with Between and In variants
2. Update parse_filter_clauses() to recognize BETWEEN/IN syntax
3. Add to planner type validator: new error scenarios
4. Parse IN list and validate operand types
5. Parse BETWEEN bounds and type-check against column
6. Test: 12+ planner tests (BETWEEN/IN recognition, type mismatches, edge cases)

### Phase 9c Step 2: Worker Execution (Est. 6-8 hours)
1. Extend filter evaluation logic to handle BETWEEN and IN operators
2. Implement BETWEEN evaluation: `value >= lower AND value <= upper`
3. Implement IN evaluation: iterate list, check equality
4. Handle NULL operands: NULL BETWEEN/IN → NULL (skip row evaluation)
5. Pre-validation: type check all operands against column canonical_type
6. Test: 18+ worker integration tests (positive/negative/edge cases)

### Phase 9c Step 3: Error Taxonomy (Est. 2-3 hours)
1. Add InvalidBetweenBounds, InListTypeHomogeneity error variants
2. Extend format_type_error() for new scenarios
3. Extend format_planner_type_error() for new scenarios
4. Error messages include: column name, expected type, actual types of operands
5. Test: error message format validation

### Phase 9c Step 4: Integration & Testing (Est. 4-5 hours)
1. Positive path: queries with valid BETWEEN/IN predicates
2. Negative path: type mismatches, invalid bounds, empty sets
3. Combination: BETWEEN + JOIN, BETWEEN + GROUP BY, BETWEEN + ORDER BY
4. NULL semantics: NULL operand, NULL column value, NULL in IN list
5. Test: 25+ comprehensive integration tests
6. Quality gates: fmt ✓, check ✓, clippy ✓

**Total Phase 9c effort**: 16-22 hours

## Vertical Slice Anatomy

- **Client**: No changes (BETWEEN/IN syntax already forwarded)
- **Server**: Query dispatch unchanged (predicate validation already in planner)
- **Planner**: Recognize BETWEEN/IN patterns, validate types (Step 1)
- **Worker**: Execute BETWEEN/IN operators, enforce type coercion (Step 2)
- **Flight**: Return results from BETWEEN/IN queries (no changes needed)

## NULL Semantics

### BETWEEN NULL Handling
- `NULL BETWEEN x AND y` → NULL (not matched)
- `col BETWEEN NULL AND y` → NULL (undefined range)
- `col BETWEEN x AND NULL` → NULL (undefined range)
- Semantics: any NULL operand → entire predicate NULL → row excluded

### IN NULL Handling
- `NULL IN (a, b, c)` → NULL (not equal to any)
- `col IN (a, b, NULL)` → NULL if col matches nothing (unknown if col = NULL)
- Semantics: NULL in list is meaningful (outer join semantics)
- Standard SQL: `col IN (1, NULL)` with col=2 → NULL (not true, not false)

## Risk Mitigation

### Risk 1: Type Mismatch in IN List
**Mitigation**: Validate all IN operands against column canonical_type; reject if any mismatch (Strict policy).

### Risk 2: BETWEEN Bounds Order
**Mitigation**: At planning time, if both bounds are constants, validate lower ≤ upper.

### Risk 3: NULL Semantics Divergence
**Mitigation**: Document NULL behavior in comments; comprehensive NULL test coverage.

### Risk 4: Performance Degradation
**Mitigation**: IN operator with large list (100+) could be slow; defer optimization to Phase 9.5+ (index hints).

## Testing Strategy

### Planner Tests (filter_type_checker.rs extensions)
- BETWEEN recognition with valid column + types
- IN list with homogeneous types
- Type mismatches in BETWEEN bounds
- Type mismatches in IN list values
- NULL operands in BETWEEN/IN
- Invalid BETWEEN bounds (literal constants where lower > upper)

### Worker Tests (execution_type_coercion_tests.rs extensions)
- Positive: BETWEEN matches rows in range
- Positive: IN matches rows in list
- Negative: BETWEEN type mismatches
- Negative: IN list type mismatches
- Edge: NULL operands
- Edge: Empty IN list (always false)
- Edge: BETWEEN with same bounds (col = value)
- Integration: BETWEEN/IN with JOIN, GROUP BY, ORDER BY

## Architecture Decisions

### Decision 1: Logical Representation
- **Option A**: Extend PhysicalExpr with Between/In variants (explicit, semantic)
- **Option B**: Expand BETWEEN → `col >= lower AND col <= upper` at parser (simpler, reuses AND logic)
- **Chosen**: Option A (explicit operators are clearer for diagnostics and future optimization)

### Decision 2: IN List Type Validation
- **Option A**: Validate all IN operands match column type (Strict)
- **Option B**: Implicit coercion within IN list (violated Strict policy)
- **Chosen**: Option A (enforce Strict consistently with Phase 9b)

### Decision 3: OR Handling
- **Option A**: Support BETWEEN/IN with OR (e.g., `WHERE x > 0 OR x IN (...)`)
- **Option B**: Phase 9c supports AND-only; OR deferred
- **Chosen**: Option B (Phase 9 predicate constraints frozen; OR support Phase 10+)

## Success Criteria
1. 12+ planner tests pass (BETWEEN/IN recognition, type validation)
2. 18+ worker tests pass (positive/negative/edge cases)
3. All integration tests pass (HEAD, GROUP BY, ORDER BY, JOIN contexts)
4. All quality gates pass (fmt ✓, check ✓, clippy ✓)
5. Error messages provide column name + expected/actual types
6. NULL semantics match standard SQL behavior
7. Phase 9b integration tests still pass (backward compatibility)

## Phase Signoff Criteria
- [x] Discovery complete (this document)
- [ ] Step 1 planner support complete with test coverage
- [ ] Step 2 worker execution complete with test coverage  
- [ ] Step 3 error taxonomy complete
- [ ] Step 4 integration and comprehensive testing complete
- [ ] All mandatory quality gates passing
- [ ] Matrix completed with evidence links
- [ ] Final signoff recorded in ROADMAP2_PHASE9C_MATRIX.md
