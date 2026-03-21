# Phase 9c Implementation Plan: BETWEEN/IN Predicates

## Overview
Phase 9c delivers BETWEEN and IN predicate support across the query pipeline, building on Phase 9b Type Coercion Enforcement foundation. This plan tracks 4 sequential steps from planner validation through integration testing.

## Scope
- **Predicates**: BETWEEN (`col BETWEEN lower AND upper`) and IN (`col IN (val1, val2, ...)`)
- **Type Policy**: Strict coercion enforcement (Phase 9b foundation)
- **NULL Semantics**: Standard SQL (NULL operand excludes row)
- **Backward Compatibility**: Phase 9a/9b predicates remain fully supported

## Implementation Steps

### Step 1: Planner Support (COMPLETE ✅)
**Date Completed**: March 21, 2026

**What**: Extended planner-side validation to recognize and type-check BETWEEN and IN predicates.

**Implementation**:
- Extended `validate_raw_sql_predicate()` in `kionas/src/planner/filter_type_checker.rs` with BETWEEN/IN pattern detection
- Added 3 new helper functions:
  1. `infer_operand_type(literal: &str) -> Option<String>` (lines 199-223)
     - Recognizes bool (true/false), int (numeric), string (quoted) literals
     - Returns type name or None for unrecognized formats
  
  2. `validate_between_predicate(sql, column_types)` (lines 225-285)
     - Pattern: "column BETWEEN lower AND upper"
     - Extracts and validates: column exists, bounds are inferable, bound types match
     - Returns TypeCoercionViolation on type mismatch or parse failure
  
  3. `validate_in_predicate(sql, column_types)` (lines 287-355)
     - Pattern: "column IN (val1, val2, ...)"
     - Extracts and validates: column exists, all values are inferable, values are homogeneous
     - Returns TypeCoercionViolation if values have different types

- Case-insensitive keyword detection with proper string slicing and trimming
- Type homogeneity validation: all bounds/values must have identical types
- Column existence pre-validation before type checking

**Test Coverage** (14 comprehensive tests):
- `accepts_between_with_integer_bounds()` - basic BETWEEN with ints
- `accepts_between_with_string_bounds()` - basic BETWEEN with strings
- `rejects_between_with_missing_column()` - column existence check
- `rejects_between_with_mismatched_bound_types()` - type homogeneity (int vs string)
- `rejects_between_with_invalid_lower_bound()` - unparseable literal detection
- `accepts_in_with_integer_list()` - basic IN with ints
- `accepts_in_with_string_list()` - basic IN with strings
- `accepts_in_with_single_value()` - IN with 1 value (edge case)
- `rejects_in_with_missing_column()` - column existence check
- `rejects_in_with_heterogeneous_types()` - type homogeneity (mixed types)
- `rejects_in_with_empty_list()` - empty IN list rejection
- `rejects_in_with_invalid_literals()` - unparseable literals
- `accepts_between_with_case_insensitive_keyword()` - keyword case handling
- `accepts_in_with_case_insensitive_keyword()` - keyword case handling
- `accepts_between_with_decimal_column()` - decimal type support (bonus)
- `accepts_in_with_multiple_spaces_in_list()` - whitespace handling (bonus)

**Quality Gates**: ✅ fmt, ✅ check -p kionas, ✅ clippy

**Evidence**:
- [kionas/src/planner/filter_type_checker.rs](../../../kionas/src/planner/filter_type_checker.rs) (modified)
  - Lines 199-223: `infer_operand_type()` function
  - Lines 225-285: `validate_between_predicate()` function
  - Lines 287-355: `validate_in_predicate()` function
  - Lines 130-135: BETWEEN/IN pattern routing in `validate_raw_sql_predicate()`
  - Lines 460-640: 16 test cases verifying all patterns

**Effort**: 5-6 hours (implementation + testing + quality gates)

### Step 2: Worker Execution (PENDING)
**Status**: Not started

**Scope**: 
- Extend worker runtime to evaluate BETWEEN and IN operators
- Implement NULL semantics: NULL operand excludes row
- Type-safe evaluation using schema_metadata from Phase 9b

**Estimated Effort**: 6-8 hours

### Step 3: Error Taxonomy (PENDING)
**Status**: Not started

**Scope**:
- Extend error taxonomy for bounds validation (lower ≤ upper for BETWEEN)
- Add list homogeneity error messages
- Remediation suggestions for common mistakes

**Estimated Effort**: 2-3 hours

### Step 4: Integration Testing (PENDING)
**Status**: Not started

**Scope**:
- End-to-end tests: client → server → worker → results
- Vertical slice validation with real data
- Deterministic output verification
- NULL semantics coverage in complex queries

**Estimated Effort**: 4-5 hours

## Blockers and Dependencies
- **Blocked By**: None (all Phase 9b items complete)
- **Blocks**: Step 2 requires Step 1 completion (satisfied)
- **Related**: Phase 9b Type Coercion (foundation)

## Critical Path
1. Step 1: Planner support (COMPLETE ✅)
2. Step 2: Worker execution (next)
3. Step 3: Error taxonomy
4. Step 4: Integration testing

## Next Action
Begin Phase 9c Step 2: Extend worker execution engine to evaluate BETWEEN and IN operators with strict type validation and NULL semantics.
