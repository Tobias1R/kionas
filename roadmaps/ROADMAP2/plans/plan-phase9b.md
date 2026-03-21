# Phase 9b: Type Coercion Enforcement - Implementation Plan

## Overview
Phase 9b enforces strict type coercion policies on WHERE predicates to prevent silent semantics bugs. Foundation laid in Phase 9a (IS/IS NOT operator support). This phase adds schema-aware type validation at query planning and runtime.

## Scope
- **In scope**: Type coercion validation for Filter predicates, NULL semantics for all supported predicates, column type contract attachment to physical plans
- **Out of scope**: Implicit type coercions (int→decimal), function type signatures, scalar expression type checking, complex predicates
- **Platform requirement**: DecimalCoercionPolicy::Strict enforcement (schema-aligned, no silent fallback)

## Blockers Resolved
1. ✅ Filter operators carry no schema metadata (now: PhysicalPlan carries optional schema_metadata)
2. ✅ Type coercion not validated post-parsing (now: validator checks column types when schema available)
3. ✅ No error taxonomy for type violations (now: TypeCoercionViolation error variant added)

## Implementation Steps

### Step 4: Schema Metadata Plumbing (COMPLETE)
**Objective**: Extend physical plan to carry schema metadata through query pipeline

**Tasks**:
- [x] Add `schema_metadata: Option<HashMap<String, ColumnDatatypeSpec>>` to PhysicalPlan struct
- [x] Update all PhysicalPlan constructors (physical_translate, physical_validate, explain, distributed_plan tests)
- [x] Ensure serialization/deserialization backward compatible with `#[serde(default)]`
- [x] Quality gates: fmt, check, clippy

**Time estimate**: 2-3 hours
**Completed**: March 21, 2026

### Step 5: Validator Type Checking (COMPLETE)
**Objective**: Add Filter predicate type validation at physical plan validation stage

**Tasks**:
- [x] Create filter_type_checker module with column type validation logic
- [x] Add TypeCoercionViolation error variant to PlannerError enum
- [x] Implement check_filter_predicate_types() to validate predicates against schema
- [x] Recognize Phase 9a patterns (IS NULL, IS NOT NULL, NOT (IS NULL))
- [x] Recognize Phase 9b patterns (comparison operators: =, <, >, <=, >=, !=)
- [x] Integrate into validate_physical_plan() with graceful fallback when schema_metadata absent
- [x] Add 9 comprehensive test cases (column refs, IS predicates, comparisons, negation, edge cases)
- [x] Quality gates: fmt, check, clippy

**Time estimate**: 4-5 hours
**Completed**: March 21, 2026

### Step 6: Worker Runtime Validation (COMPLETE)
**Objective**: Mirror validator logic at worker execution time for runtime type enforcement

**Tasks**:
- [x] Extend worker query runtime to accept and use schema_metadata from PhysicalPlan
- [x] Add schema_metadata parameter to worker query execution context
- [x] Pre-validate Filter operand types before SQL evaluation
- [x] Emit actionable TypeCoercionViolation errors with column names, expected types, actual types
- [x] Integration tests showing type validation at worker runtime
- [x] Documentation for breaking change: queries with type mismatches now rejected

**Time estimate**: 6-8 hours
**Completed**: March 21, 2026
**Evidence**:
- Enhanced validate_filter_predicates_types() function (worker/src/services/query_execution.rs)
- Type inference and compatibility validation helpers
- Integrated into apply_filter_pipeline() with optional schema_metadata
- All quality gates passing

### Step 7: Type Coercion Test Expansion (COMPLETE)
**Objective**: Build comprehensive test coverage for type coercion enforcement

**Tasks**:
- [x] Positive path tests: predicates with matching types (int to int column, decimal to decimal, etc.)
- [x] Negative path tests: predicates with mismatched types (numeric literal to string column, etc.)
- [x] NULL handling tests: NULL operands compatible with any column type
- [x] Edge cases: empty schema, missing columns, complex predicates with AND/OR
- [x] Integration with existing IS/IS NOT tests from execution_is_operator_tests.rs
- [x] Deterministic output validation for accepted queries

**Time estimate**: 4-6 hours
**Completed**: March 21, 2026
**Evidence**:
- execution_type_coercion_tests.rs created (worker/tests/ directory, 350+ lines)
- 18+ test cases covering positive, negative, and edge scenarios
- All quality gates passing

### Step 8: Error Taxonomy & Remediation (COMPLETE)
**Objective**: Define error taxonomy and provide actionable remediation guidance

**Tasks**:
- [x] Enumerate all type coercion error scenarios (column missing, type mismatch, NULL context)
- [x] Add new error functions: format_type_error() in worker, format_planner_type_error() in planner
- [x] Error message format: `type coercion violation: column "{column}" expects {type_spec} but got {operand_value} (type {operand_type}). remediations: [suggestions]`
- [x] Include precision/scale in type spec for decimal columns (e.g., "DECIMAL(10,2)")
- [x] Error propagation: validator errors stop query; worker errors emit per-batch error payloads
- [x] Testing: all error paths covered with expected message validation

**Time estimate**: 3-4 hours
**Completed**: March 21, 2026
**Evidence**:
- format_type_error() with scenarios (worker/src/services/query_execution.rs)
- format_planner_type_error() with same scenarios (kionas/src/planner/filter_type_checker.rs)
- Remediation suggestions integrated into error messages
- All quality gates passing

## Architecture Decisions

### Metadata Attachment: PhysicalPlan Struct Extension (Ratified)
- ✅ Extend PhysicalPlan struct (not operator-specific)
- ✅ Carries metadata for all operators, not just Filter
- ✅ Minimal disruption (optional field, backward compatible)
- ✅ Serializable/deserializable without external dependencies

### Metadata Population: Server-Side (Deferred to Phase 9.5)
- Current: Schema metadata starts as None for all queries
- Phase 9b: Validation logic present but skipped when metadata absent (interim)
- Phase 9.5: Wire server-side metadata population (MetastoreClient → PhysicalPlan.schema_metadata)
- Worker: Receives pre-populated PhysicalPlan from server

### Type Validation: Layered Approach
- **Layer 1 (Step 5)**: Column existence validation (validator stage) → fails fast on wrong column names
- **Layer 2 (Step 6)**: Type compatibility validation (validator + worker) → fails on type mismatches
- **Layer 3 (Step 8)**: Remediation guidance in error messages

## Vertical Slice Anatomy
- **Client**: No changes (query submission unchanged)
- **Server**: Passes physical plan with schema_metadata (will be populated in Phase 9.5)
- **Planner**: Validates predicates against schema (Step 5 ✅)
- **Worker**: Enforces type coercion at filter evaluation (Step 6 ✅)
- **Flight**: Returns TypeCoercionViolation errors in error payloads (Step 8 ✅)

## Backward Compatibility
- ✅ Phase 9a predicates remain supported (IS NULL/NOT NULL)
- ✅ Existing queries without schema_metadata continue to work (type checking skipped)
- ⚠️ **Breaking change** (deferred to Phase 9.5): Once server-side metadata populated, queries with type mismatches will be rejected

## Testing Strategy

### Validator Tests (physical_validate.rs)
- Simple column references with/without schema
- IS NULL/IS NOT NULL patterns on existing/missing columns
- Comparison operators (=, <, >, etc.) with existing/missing columns
- Negation patterns (NOT ...)
- Edge cases (empty schema, special characters in column names)

### Worker Integration Tests (execution_is_operator_tests.rs extensions)
- Type-matching predicates: queries accepted and evaluated correctly
- Type-mismatching predicates: queries rejected with TypeCoercionViolation
- NULL semantics: NULL operands work with any column type
- Combined predicates: AND/OR with mixed type-valid/invalid operands

### Error Path Tests (error_tests module)
- All error message formats validated
- Remediation suggestions present and actionable
- Error propagation through distributed plan stages

## Effort Estimates
- **Step 4**: 2-3 hours (COMPLETE March 21)
- **Step 5**: 4-5 hours (COMPLETE March 21)
- **Step 6**: 6-8 hours (COMPLETE March 21)
- **Step 7**: 4-6 hours (COMPLETE March 21)
- **Step 8**: 3-4 hours (COMPLETE March 21)
- **Total Phase 9b**: ~19-24 hours (ALL STEPS COMPLETE)

## Success Criteria
✅ All 9 validator tests pass (Step 5)
✅ Worker runtime accepts populated schema_metadata and validates types (Step 6)
✅ 18+ comprehensive type coercion tests implemented and passing (Step 7)
✅ Error taxonomy with remediation guidance implemented (Step 8)
✅ All quality gates passing: fmt ✓, check ✓, clippy ✓
✅ Phase 9b signoff complete - ready for Phase 9c (BETWEEN/IN predicates)
3. Type mismatch queries rejected with clear error messages
4. All test suites passing with strict clippy/fmt/check gates
5. Phase 9b completion matrix signed off with mandatory criteria marked Done

## Files Modified/Created
- ✅ [kionas/src/planner/physical_plan.rs](../../../../kionas/src/planner/physical_plan.rs) - Added schema_metadata field
- ✅ [kionas/src/planner/error.rs](../../../../kionas/src/planner/error.rs) - Added TypeCoercionViolation variant
- ✅ [kionas/src/planner/filter_type_checker.rs](../../../../kionas/src/planner/filter_type_checker.rs) - New module (validation logic)
- ✅ [kionas/src/planner/physical_validate.rs](../../../../kionas/src/planner/physical_validate.rs) - Integrated type checking
- 🔄 [kionas/src/planner/mod.rs](../../../../kionas/src/planner/mod.rs) - Added filter_type_checker module declaration
- 📝 [worker/src/execution/query.rs](../../../worker/src/execution/query.rs) - (Step 6: extend with type validation)
- 📝 Additional test modules TBD

## Next Phase Transition
- Phase 9b Step 6+ must complete before Phase 9c (BETWEEN/IN) begins
- Phase 9c depends on type validation framework being stable
- Phase 9.5 (server metadata population) can proceed in parallel with Step 6-8
