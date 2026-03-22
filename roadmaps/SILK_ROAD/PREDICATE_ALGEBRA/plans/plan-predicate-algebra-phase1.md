# Plan: Structured Predicates Implementation (5 Phases)

## Overview

**Goal:** Replace raw SQL string predicates with structured, typed PredicateExpr objects for clean architecture, type safety, and extensibility.

**Scope:** Phases 1-5 (concurrent work possible after Phase 1 is merged)

**Total Effort:** 13–18 hours across 5 plans

**Sequencing:** Phase 1 (proto) → Phase 2 (server translation) || Phase 3 (worker execution) → Phase 4 (integration) → Phase 5 (tests)

---

## Phase 1: Protobuf & Message Design (2–3 hours)

### Objective
Define the PredicateExpr type hierarchy and ComparisonOperator enum in protobuf format.

### Steps

1. **Create filter_predicate.proto** — [kionas/proto/worker_service.proto](kionas/proto/worker_service.proto) Enhance Task message to include new filter field; [kionas/proto/filter_predicate.proto](kionas/proto/filter_predicate.proto) (NEW FILE) for predicate definitions
   - Define `FilterPredicate`  as root oneof (Conjunction, Disjunction, Comparison, Between, In, IsNull, IsNotNull, Not)
   - Define `Conjunction { repeated FilterPredicate clauses }` — AND combinator
   - Define `Disjunction { repeated FilterPredicate clauses }` — OR combinator
   - Define `Comparison { string column_name, ComparisonOperator operator, FilterValue value, DataType column_type }`
   - Define `Between { string column_name, FilterValue lower, FilterValue upper, DataType column_type, bool is_negated }`
   - Define `In { string column_name, repeated FilterValue values, DataType column_type }`
   - Define `IsNull { string column_name }` and `IsNotNull { string column_name }`
   - Define `Not { FilterPredicate predicate }` — negation of nested predicate
   - Define `FilterValue` oneof: int_value, string_value, bool_value, float_value, double_value (add more types as needed per existing DataType)
   - Define `ComparisonOperator` enum: EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
   - Define or reuse `DataType` enum: INT32, INT64, FLOAT, DOUBLE, STRING, BOOL, DATE, TIMESTAMP, DECIMAL, (align with existing schema_metadata types)

2. **Compile protobuf** 
   - Add to [kionas/build.rs](kionas/build.rs): Compile filter_predicate.proto to Rust types
   - Verify `.rs` files generated in `kionas/generated/`
   - Ensure Cargo.toml includes `prost`, `prost-types` dependencies

3. **Create Rust wrapper types** — [kionas/src/planner/predicate_expr.rs](kionas/src/planner/predicate_expr.rs) (NEW FILE)
   - Re-export generated protobuf types (or thin wrapper if needed for methods)
   - Implement helper constructors:
     - `PredicateExpr::comparison(column, operator, value, type) → Self`
     - `PredicateExpr::between(column, lower, upper, type, negated) → Self`
     - `PredicateExpr::in_list(column, values, type) → Self`
     - `PredicateExpr::and(clauses) → Self` — builds Conjunction
     - `PredicateExpr::or(clauses) → Self` — builds Disjunction
     - `PredicateExpr::is_null(column) → Self`
     - `PredicateExpr::is_not_null(column) → Self`
   - Add validation methods:
     - `fn validate(&self) → Result<(), PredicateError>` — checks well-formedness

4. **Update worker_service.proto**
   - Modify `Task` message: change or add `filter: Optional<FilterPredicate>` field (alongside or replacing current filter_sql string)
   - Remove any form of raw SQL filter flowing to worker.

### Verification
- ✅ `cargo build -p kionas` — compiles without errors
- ✅ `cargo doc -p kionas` — generates documentation for FilterPredicate types
- ✅ Protobuf types are discoverable in `kionas::planner::predicate_expr`

### Files Modified/Created
- `kionas/proto/filter_predicate.proto` (NEW)
- `kionas/src/planner/predicate_expr.rs` (NEW)
- `kionas/proto/worker_service.proto` (MODIFIED: add filter field)
- `kionas/build.rs` (MODIFIED: add filter_predicate.proto compilation)

---

## Phase 2: Server-Side Translation Expr → PredicateExpr (4–5 hours)

### Objective
Convert logical Expr trees (from sqlparser) into structured PredicateExpr objects with full type validation.

### Steps

1. **Create predicate_translator.rs** — [kionas/src/planner/predicate_translator.rs](kionas/src/planner/predicate_translator.rs) (NEW FILE)
   - Function: `pub fn translate_expr_to_predicate_expr(expr: Expr, schema: &SchemaMetadata) → Result<PredicateExpr, TranslationError>`
   - Implement pattern matching on Expr variants:
     - `BinaryOp(left, And, right)` → `PredicateExpr::and([translate_left, translate_right])`
     - `BinaryOp(left, Or, right)` → `PredicateExpr::or([translate_left, translate_right])`
     - `BinaryOp(col, op, literal)` where op ∈ {Eq, Gt, Ge, Lt, Le, NotEq} → `PredicateExpr::comparison(...)`
     - `Between {...}` → `PredicateExpr::between(...)`
     - `InList {...}` → `PredicateExpr::in_list(...)`
     - `IsNull(col)` → `PredicateExpr::is_null(...)`
     - `IsNotNull(col)` → `PredicateExpr::is_not_null(...)`
     - `UnaryOp(Not, nested)` → `PredicateExpr::not(translate_nested)`
   - Type resolution:
     - Look up column type in schema_metadata (already threaded in Phase 9b)
     - Validate literal types against column types (use existing type coercion rules from Phase 9b)
     - Return error with column name + expected type + actual literal type for mismatches
   - Error handling:
     - Unsupported operators (LIKE, REGEXP, etc.) → descriptive error suggesting deferral to Phase 10+
     - Unknown columns → error with suggestion (column not found; did you mean X?)
     - Type mismatches → clear error with types

2. **Integrate into planner** — [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs) (MODIFIED)
   - In `translate_filter_expr()` or similar function:
     - Call `translate_expr_to_predicate_expr(filter_expr, schema_metadata)` instead of converting to string
     - Return `PhysicalExpr::Predicate(PredicateExpr)` instead of `PhysicalExpr::Raw { sql: String }`
   - Ensure filter_expr has access to schema_metadata (likely via ExecutionContext or similar; hook into existing threading)

3. **Handle unsupported operators gracefully**
   - Create list of unsupported operators: LIKE, REGEXP, scalar functions, etc.
   - Return error: "Operator LIKE is not yet supported in structured predicates; planned for Phase 10"
   - Fallback option: For now, could still use raw SQL for unsupported ops (hybrid approach during migration)

4. **Operator mapping** — Ensure sqlparser.rs BinaryOp → ComparisonOperator is correct
   - Create enum mapping: `fn binary_op_to_comparison(op: &BinaryOperator) → Result<ComparisonOperator>`
   - Map: Eq → EQUAL, NotEq → NOT_EQUAL, Gt → GREATER_THAN, Ge → GREATER_EQUAL, Lt → LESS_THAN, Le → LESS_EQUAL
   - Return error for unsupported binary ops

5. **Test translation** (unit tests in same file)
   - Simple comparisons: `id = 1`, `price > 100`
   - Compound: `id != 1 AND price BETWEEN 50 AND 500`
   - IN lists: `category IN ('Electronics', 'Furniture')`
   - NULL checks: `name IS NULL`, `description IS NOT NULL`
   - Type validation: `price = 'string_literal'` → error (type mismatch)

### Verification
- ✅ `cargo check -p kionas` — no errors
- ✅ `cargo clippy -p kionas` — no clippy warnings
- ✅ Error messages are clear and actionable
- X DONT EXECUTE cargo test.

### Files Modified/Created
- `kionas/src/planner/predicate_translator.rs` (NEW)
- `kionas/src/planner/physical_translate.rs` (MODIFIED: integrate translator)
- `kionas/src/planner/mod.rs` (MODIFIED: export predicate_translator)

---

## Phase 3: Worker-Side Execution PredicateExpr → Boolean Mask (3–4 hours)

### Objective
Implement PredicateExpr evaluation at the worker without any string parsing.

### Steps

1. **Create predicate_executor.rs** — [worker/src/services/predicate_executor.rs](worker/src/services/predicate_executor.rs) (NEW FILE)
   - Function: `pub fn evaluate_predicate(predicate: &FilterPredicate, batch: &RecordBatch) → Result<BooleanArray>`
   - Pattern match on FilterPredicate variant:
     - `Conjunction { clauses }` → AND all clause evaluations bitwise
     - `Disjunction { clauses }` → OR all clause evaluations bitwise
     - `Comparison { column, operator, value, type }` → delegate to `evaluate_comparison()`
     - `Between { column, lower, upper, type }` → delegate to `evaluate_between()`
     - `In { column, values, type }` → delegate to `evaluate_in()`
     - `IsNull { column }` → delegate to `evaluate_is_null()`
     - `IsNotNull { column }` → delegate to `evaluate_is_not_null()`
     - `Not { predicate }` → negate result of nested predicate

2. **Implement comparison operators** — `evaluate_comparison()`
   - For each DataType (Int32, Int64, Float, Double, String, Bool, Date, Timestamp):
     - Extract column as typed array (e.g., `batch.column("id").as_int32_array()`)
     - Extract literal value as typed (e.g., `value.as_int64()` and cast/coerce to Int32)
     - Apply operator: `==`, `!=`, `>`, `>=`, `<`, `<=` using Arrow compute functions
     - Return Boolean array with result for each row

3. **Implement BETWEEN evaluation** — `evaluate_between()`
   - Extract column, lower bound, upper bound as typed values
   - Compute: `lower ≤ column ≤ upper` (or `¬(...)` if is_negated)
   - Return Boolean array

4. **Implement IN evaluation** — `evaluate_in()`
   - Extract column as typed array
   - Extract values list as typed array or set
   - For each row in column, check membership in values
   - Return Boolean array

5. **Handle NULL semantics** (aligned with Phase 9b)
   - NULL in a comparison clause → row excluded (standard SQL behavior)
   - `NULL = NULL` → NULL → excluded
   - `NULL > 5` → NULL → excluded
   - `NULL IS NULL` → TRUE → included
   - `column IS NULL` → returns NULL bitmap directly

6. **Integrate into worker query execution** — [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) (MODIFIED)
   - Replace calls to `parse_filter_clauses(filter_sql)` with `deserialize_and_evaluate_predicate(filter_bytes, batch)`
   - Remove `split_and_respecting_between()` and `parse_filter_clauses()` functions (they're no longer needed once Phase 4 completes migration)
   - Call `predicate_executor::evaluate_predicate(predicate, batch)` instead

7. **Error handling**
   - Column not found in batch → error with suggestion
   - Type mismatch at runtime (shouldn't happen if Phase 2 validated correctly, but defensive check anyway)
   - Null handling edge cases → clear error messages

### Verification
- ✅ `cargo check -p worker` — no errors
- ✅ `cargo test predicate_executor` — all unit tests pass
- ✅ Test combinations: conjunction of between+in, negation of in, etc.
- ✅ `cargo clippy -p worker` — no warnings

### Files Modified/Created
- `worker/src/services/predicate_executor.rs` (NEW)
- `worker/src/services/query_execution.rs` (MODIFIED: integrate executor, remove old string parsing)
- `worker/src/services/mod.rs` (MODIFIED: export predicate_executor)

---

## Phase 4: Integration & Migration (2–3 hours)

### Objective
Plumb PredicateExpr through full server-to-worker pipeline; ensure backward compatibility during migration.

### Steps

1. **Update server execution path** — [server/src/execution.rs](server/src/execution.rs) (or similar dispatch logic)
   - Retrieve PhysicalPlan with PredicateExpr (output of Phase 2)
   - Build Task protobuf:
     - Include new `filter: Some(FilterPredicate { ... })` field
     - Optionally keep `filter_sql: String` field for now (migration phase)
   - Serialize Task → send to worker

2. **Update worker task deserialization** — [worker/src/execution/planner.rs](worker/src/execution/planner.rs) or task handling code
   - Deserialize incoming Task protobuf
   - Check: if new `filter` field exists, use structured predicate path
   - Fallback: if `filter_sql` field still present, use old string-parsing path (for backward compat)
   - Log which path is being used (INFO level): "Using structured predicate execution" or "Using legacy raw SQL parsing"

3. **Deprecation timeline**
   - For first release: Both paths coexist
   - Update changelog: "New: Structured predicate execution (improves performance & reduces bugs)"
   - For next release: Warn on use of raw SQL filter_sql (deprecation warning)
   - For release after: Remove raw SQL path entirely

4. **Update configuration/feature flags** (if needed)
   - Consider: env var to force old parsing for debugging: `KIONAS_USE_LEGACY_FILTER_SQL=1`

5. **Integration tests** 
   - End-to-end test: client submits query → server planner translates to PredicateExpr → worker executes → results correct
   - Test both simple and complex predicates

### Verification
- ✅ `cargo build` — server + worker compile
- ✅ Integration tests pass
- ✅ Backward compatibility: old systems still work (if filter_sql path kept)
- ✅ New path produces identical results to old path for all test cases

### Files Modified/Created
- `server/src/execution.rs` (MODIFIED: build PredicateExpr in Task)
- `worker/src/execution/planner.rs` (MODIFIED: deserialize PredicateExpr)
- Integration tests (NEW: test full pipeline)

---

## Phase 5: Testing & Validation (2–3 hours)

### Objective
Comprehensive test coverage for all operator combinations, edge cases, and regression prevention.

### Steps

1. **Unit tests for predicate_translator.rs**
   - Simple operators: `id = 1`, `price != 100`, `name > 'a'`, etc.
   - Compound predicates: `a AND b AND c`, `a OR (b AND c)`, `NOT(a OR b)`
   - BETWEEN: `col BETWEEN 1 AND 10`, `col NOT BETWEEN 1 AND 10`
   - IN: `col IN (1, 2, 3)`, `col IN ('a', 'b')`
   - NULL checks: `col IS NULL`, `col IS NOT NULL`
   - Type validation errors: `id = 'string'` → error
   - Unknown column: `unknown_col = 1` → error with suggestion
   - Unsupported operator: `col LIKE '%pattern'` → error with Phase 10 reference

2. **Unit tests for predicate_executor.rs**
   - Construct FilterPredicate directly (no server)
   - Create mock RecordBatch with test data
   - Call evaluate_predicate() for each case
   - Assert Boolean array results match expected mask
   - Test combinations: (BETWEEN AND IN), (NOT (IN OR BETWEEN)), etc.
   - NULL handling: verify NULL values are handled per SQL semantics
   - Edge cases: empty IN list, single-value IN list, bounds swapped in BETWEEN, etc.

3. **Integration tests** — Full pipeline end-to-end
   - Phase 9c test statements (now using structured predicates):
     - Statement 25: `WHERE price BETWEEN 50 AND 500 AND category IN ('Electronics', 'Furniture')`
     - Statements 12, 18: `WHERE id != 1`, `WHERE category != 'Furniture'`
   - Execute full phase9_simple_test.sql using new structured predicate path
   - Verify 24/24 statements pass (Statement 4 removed as DROP TABLE out of Phase 9c scope)

4. **Regression tests** 
   - Run existing test suites (Phases 3-6, earlier phase tests)
   - Verify no performance degradation (new path is actually faster: 1 parse instead of 2)
   - Ensure Phase 9b type coercion tests still pass

5. **Stress tests**
   - Large IN lists: `col IN (1, 2, ..., 1000)`
   - Deep nesting: `(a AND (b OR (c AND d)))`
   - Many conjunctions: `a AND b AND c AND d AND e AND f AND g`

### Verification
- ✅ Phase 9c test statements run successfully
- ✅ No regressions in existing tests
- ✅ Code coverage >90% for new modules

### Files Modified/Created
- `kionas/src/planner/tests/predicate_translator_tests.rs` (NEW)
- `worker/src/services/tests/predicate_executor_tests.rs` (NEW)
- `tests/integration/predicate_e2e_tests.rs` (NEW, or integrate into existing e2e suite)

---

## Implementation Sequence & Parallelization

### Critical Path
```
Phase 1 (Proto Design) [BLOCKING]
  ↓
Phase 2 (Server Translation) + Phase 3 (Worker Execution) [PARALLEL after Phase 1]
  ↓
Phase 4 (Integration)
  ↓
Phase 5 (Testing)
```

### Recommended Sequence
1. **Day 1:** Phase 1 (2-3 hours) + Phase 2 start (1 hour) = stop at end of Phase 2 morning
2. **Day 2:** Phase 2 finish (3 hours) + Phase 3 (3-4 hours in parallel where possible)
3. **Day 3:** Phase 3 finish + Phase 4 (2-3 hours) + Phase 5 start (1 hour)
4. **Day 4:** Phase 5 finish (2-3 hours) + integration tests + final verification

**Total Calendar Time:** ~2.5 working days if focused; ~3-4 days with part-time work

---

## Success Criteria

✅ All 5 phases completed and merged
✅ phase9_simple_test.sql: 24/24 statements pass using structured predicates
✅ No regressions in existing test suites
✅ Server planner generates PredicateExpr without errors
✅ Worker executor runs PredicateExpr without string parsing
✅ Error messages are clear and actionable
✅ Code passes: `cargo fmt`, `cargo clippy -D warnings`, `cargo check`
✅ Documentation in code via rustdocs
✅ Clear deprecation timeline for raw SQL parsing path documented

---

## Open Decisions

1. **Should we keep raw SQL path for backward compatibility?**
   - Yes: Safer migration, old systems still work
   - No: Cleaner, force migration immediately
   - **Recommendation:** Yes, with deprecation timeline (1 release warn, 1 release remove)

2. **Should Phase 2 emit errors for unsupported operators, or silently fall back to raw SQL?**
   - Errors (better): Users know what's not supported
   - Fallback (pragmatic): System still works for unsupported ops
   - **Recommendation:** Errors with clear message + Phase 10 reference

3. **Should we add PredicateExpr to existing filter serialization, or create new filter field?**
   - New field (safer): No risk of breaking existing systems
   - Replace field (simpler): Single filter representation
   - **Recommendation:** New field; keep raw SQL alongside during migration

---

## References

- Discovery Document: [discovery-predicate-algebra-phase1.md](discovery-predicate-algebra-phase1.md)
- Related Phase 9c Bug Analysis: [../../discover/e2e_tests_after_phase9.md](../../discover/e2e_tests_after_phase9.md)
- Type Coercion Foundation (Phase 9b): [../../ROADMAP2/discovery/discovery-phase9b.md](../../ROADMAP2/discovery/discovery-phase9b.md)
