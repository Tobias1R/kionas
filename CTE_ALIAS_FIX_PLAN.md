# CTE Alias Projection Fix Plan

**Status**: Investigation Complete  
**Root Cause**: HIGH CONFIDENCE  
**Complexity**: MEDIUM  
**Estimated Time**: 3-4 hours investigation + fix + testing  

---

## Problem Statement

When a CTE defines a column alias (e.g., `o.product_id AS ppid2`), and an outer query references that alias in a subsequent JOIN, the planner fails with:

```
ERROR: join key 'ppid2' is not present in schema
```

**Failing Query** (testv2.sql #7):
```sql
WITH customer_orders AS (
    SELECT c.id, o.quantity, c.name, o.product_id AS ppid2
    FROM bench4.seed1.customers c
    JOIN bench4.seed1.orders o ON c.id = o.customer_id
    WHERE c.id = 700
)
SELECT * FROM customer_orders CO
JOIN bench4.seed1.products p ON p.id = CO.ppid2;  -- ❌ ppid2 not found
```

---

## Root Cause Analysis

### Primary Issue: CTE Output Schema Not Propagated

The `SelectQueryModel` structure stores CTE information but **lacks explicit column metadata**:

| Layer | What's Stored | What's Missing |
|-------|---------------|-----------------|
| **Query Model** | CTE name + SELECT text | Column alias mapping |
| **Planner** | Alias map (for CTE's internal joins) | Schema for outer query joins |
| **Physical Plan** | Join keys as strings | Link between alias & underlying column |
| **Worker Execution** | Join key names | Source table for each column |

### Secondary Issue: Alias Resolution Not Applied to Outer Joins

The planner builds `cte_aliases` mapping at **line 1195 in [server/src/planner/engine.rs](server/src/planner/engine.rs)** but only uses it for the CTE's internal joins. When the outer query joins another table, the alias map is out of scope.

### Tertiary Issue: Qualified References Lose Context

When join key is `customer_orders.ppid2`:
1. ✅ Table prefix normalized to `ppid2`
2. ✅ Found in alias map as `ppid2 → o.product_id`
3. ❌ Remapped back to `ppid2` (already exists, no new info added)
4. ❌ Execution layer receives `ppid2` with **no metadata about its origin**

---

## Impact Assessment

| Aspect | Status | Details |
|--------|--------|---------|
| **Scope** | LOCALIZED | Affects only CTE columns referenced in outer queries |
| **Frequency** | MEDIUM | Common pattern for complex queries |
| **Blocking Issues** | 1/14 E2E tests (7%) | testv2.sql Query #7 |
| **Related Issues** | POTENTIAL | Any future query using CTE aliases in JOINs |
| **Data Loss Risk** | NONE | Parsing/metadata issue, not execution |

---

## Fix Strategy

### Phase 1: Extend SelectQueryModel with Column Metadata (30 min)

**File**: [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs)

**Current State** (Lines 162-165):
```rust
pub struct QueryCteSpec {
    pub name: String,                      // "customer_orders"
    pub query: Box<SelectQueryModel>,       // CTE definition
}

pub struct SelectQueryModel {
    pub projection: Vec<String>,            // Just SQL text
    pub ctes: Vec<QueryCteSpec>,
    // ❌ MISSING: Schema metadata
}
```

**Add**:
```rust
pub struct QueryCteSpec {
    pub name: String,
    pub query: Box<SelectQueryModel>,
    // NEW: Map alias name to underlying column source
    pub column_mapping: HashMap<String, (String, String)>,  // alias -> (table, column)
}

pub struct SelectQueryModel {
    pub projection: Vec<String>,
    pub ctes: Vec<QueryCteSpec>,
    pub alias_column_map: HashMap<String, (String, String)>,  // For outer query reference
}
```

**Rationale**: Explicit metadata makes alias resolution deterministic and traceable

---

### Phase 2: Build Column Mapping During Query Model Construction (45 min)

**File**: [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs), lines 925-1012

**Location**: `build_select_query_model_from_select` function

**Changes Required**:
1. Parse projection list to extract aliases:
   - `"o.product_id AS ppid2"` → Extract `ppid2`, `o`, `product_id`
   - `"count(*) AS cnt"` → Extract `cnt`, `_aggregate`, `count(*)`

2. Build `column_mapping` for CTE:
   ```rust
   column_mapping: {
     "ppid2" -> ("o", "product_id"),
     "id" -> ("c", "id"),
     "quantity" -> ("o", "quantity"),
   }
   ```

3. Store in `QueryCteSpec` when building CTEs

**Test Case**:
- Parse query with multiple aliases
- Verify `column_mapping` has all 4-5 expected entries
- Check edge cases: functions, expressions, qualified references

---

### Phase 3: Propagate Alias Map Through Planner (1 hour)

**File**: [server/src/planner/engine.rs](server/src/planner/engine.rs), lines 1189-1250

**Changes Required**:

1. **Extract CTE column metadata** (replace lines 830-851):
   ```rust
   fn extract_cte_column_schema(
       cte_spec: &QueryCteSpec
   ) -> HashMap<String, (String, String)> {
       // Use cte_spec.column_mapping directly
       // No need to reconstruct from text
       cte_spec.column_mapping.clone()
   }
   ```

2. **Available throughout outer query planning** (lines 1195-1250):
   ```rust
   let cte_column_schema = extract_cte_column_schema(&source_model.ctes[0]);
   
   // Pass to join planning
   for join in &source_model.joins {
       resolve_join_keys(
           &join,
           &cte_column_schema,  // NEW: Add this parameter
           &physical_operators,
       );
   }
   ```

3. **Resolve join keys with fallback**:
   ```rust
   fn resolve_join_key(
       key: &str,
       cte_schema: &HashMap<String, (String, String)>,
   ) -> String {
       if let Some((table, col)) = cte_schema.get(key) {
           format!("{}.{}", table, col)  // ppid2 → o.product_id
       } else {
           key.to_string()  // Fallback to original
       }
   }
   ```

**Test Case**:
- Verify `cte_column_schema` contains all alias mappings
- Trace `ppid2` resolution to `o.product_id` through planner
- Check unqualified and qualified references both work

---

### Phase 4: Update Physical Operator to Store Column Resolution (30 min)

**File**: [kionas/src/physical_plan.rs](kionas/src/physical_plan.rs)

**Concept**: Embed alias metadata in PhysicalOperator::HashJoin or NestedLoopJoin

**Current State**:
```rust
pub struct HashJoin {
    left: Box<PhysicalOperator>,
    right: Box<PhysicalOperator>,
    keys: Vec<(String, String)>,  // ("ppid2", "id")
    // ❌ No metadata about where "ppid2" comes from
}
```

**Add** (optional, if needed by worker):
```rust
pub struct HashJoin {
    left: Box<PhysicalOperator>,
    right: Box<PhysicalOperator>,
    keys: Vec<(String, String)>,
    // NEW: Map alias to resolved column for worker
    column_resolution: HashMap<String, String>,  // {"ppid2" → "o.product_id"}
}
```

**Rationale**: Makes worker execution deterministic without re-resolving

---

### Phase 5: Update Worker Join Execution (if needed) (30 min)

**File**: [worker/src/execution/join.rs](worker/src/execution/join.rs)

**Depends on**: Whether worker can accept resolved column names from Phase 4

**Action**:
- If Phase 4 resolves fully to underlying columns → No worker changes needed
- If Phase 4 passes aliases → Update `find_column_in_batch()` to resolve aliases within the join operator

---

### Phase 6: Add Regression Tests (45 min)

**File**: 
- [server/src/tests/planner_engine_tests.rs](server/src/tests/planner_engine_tests.rs) - New test function
- [server/src/tests/phase4d_sprint2_integration_tests.rs](server/src/tests/phase4d_sprint2_integration_tests.rs) - E2E test

**Test Cases**:

**Test 1: CTE with Single Alias, Outer Query JOIN**
```rust
#[tokio::test]
async fn cte_alias_projection_in_outer_join_resolves_correctly() {
    let sql = r#"
        WITH co AS (
            SELECT c.id, o.product_id AS ppid
            FROM customers c JOIN orders o ON c.id = o.customer_id
        )
        SELECT * FROM co JOIN products p ON p.id = co.ppid
    "#;
    
    let query_model = parse_query_model(sql).unwrap();
    let plan = build_kionas_plan_from_intent(&query_model).unwrap();
    
    // Verify join keys are resolved to underlying columns
    assert_join_key_resolved(&plan, "ppid", "o.product_id");
}
```

**Test 2: Multiple Aliases, Complex Outer JOIN**
```rust
#[tokio::test]
async fn cte_multiple_aliases_in_complex_join() {
    let sql = r#"
        WITH co AS (
            SELECT 
                c.id AS cid,
                o.quantity AS qty,
                o.product_id AS ppid2
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
        )
        SELECT * FROM co
        JOIN products p ON p.id = co.ppid2
        JOIN suppliers s ON s.id = p.supplier_id
        WHERE co.qty > 5
    "#;
    
    // Verify all 3 aliases (cid, qty, ppid2) resolve correctly
    // through the multi-join chain
}
```

**Test 3: Qualified vs Unqualified Reference**
```rust
#[tokio::test]
async fn cte_alias_both_qualified_and_unqualified_refs() {
    let sql1 = "... ON p.id = co.ppid2";  // Qualified
    let sql2 = "... ON p.id = ppid2";    // Unqualified (if supported)
    
    // Both should resolve to same underlying column
}
```

**Test 4: E2E Integration (from testv2.sql)**
```rust
#[tokio::test]
async fn testv2_query_7_cte_alias_with_multi_join() {
    // Run exact failing query from testv2.sql
    // Verify result set matches expected output
}
```

---

## Implementation Sequence

### Recommended Order:
1. **Phase 1** (30 min) - Extend data structures
2. **Phase 2** (45 min) - Build column mapping during parsing
3. **Phase 6 - Test 1** (15 min) - Create minimal unit test for Phase 2
4. **Phase 3** (1 hour) - Propagate through planner
5. **Phase 6 - Tests 2-3** (30 min) - Test planner logic
6. **Phase 4** (30 min) - Update physical operator (if needed)
7. **Phase 5** (30 min) - Update worker (if needed)
8. **Phase 6 - Test 4** (15 min) - E2E test with testv2.sql Query #7

**Total**: ~3.5 hours

---

## Risk Mitigation

### Testing Strategy
- ✅ Unit test: Column mapping extraction
- ✅ Unit test: Alias resolution in planner
- ✅ E2E test: Full query execution
- ✅ Regression test: Existing CTE functionality still works

### Backward Compatibility
- ✅ Changes are additive (new fields in structs)
- ✅ Fallback to original if alias not found
- ✅ No breaking changes to worker contract

### Validation Before Merge
1. `cargo fmt --all` - Format
2. `cargo clippy --all-targets --all-features -- -D warnings` - Lint
3. New tests must pass: `cargo test --all`
4. testv2.sql Query #7 must succeed in E2E environment

---

## Success Criteria

| Criterion | Expected | Verification |
|-----------|----------|--------------|
| **Code compiles** | ✅ Yes | `cargo check` passes |
| **No new warnings** | ✅ Clean | `cargo clippy` all green |
| **Unit tests pass** | ✅ 4+ new tests | `cargo test planner_engine_tests` |
| **testv2.sql #7 passes** | ✅ Yes | Run Query #7, expect result rows |
| **Existing tests pass** | ✅ All | No regression in planner_engine_tests.rs |
| **E2E validation** | ✅ Pass | Full kionas cluster query succeeds |

---

## Known Unknowns

1. **Does worker need column resolution info?**
   - Hypothesis: If planner resolves fully to `o.product_id`, worker extracts from batch
   - Action: Test in Phase 4 implementation

2. **Are there other query patterns affected?**
   - Related: Subquery aliases, derived table aliases
   - Flag for investigation after CTE fix

3. **Performance impact of alias resolution?**
   - Expected: Negligible (HashMap lookup, no extra I/O)
   - Action: Add microbenchmark if concerned

---

## Alternative Approaches Considered

### ❌ Approach 1: Resolve Aliases at Worker Runtime
**Rejected**: Late resolution loses context; worker doesn't know alias source table

### ❌ Approach 2: Flatten CTE into Outer Query
**Rejected**: Loses CTE boundary semantics; doesn't solve general problem

### ✅ Approach 3: Propagate Schema Metadata (Recommended)
**Chosen**: Early resolution at planner; deterministic and traceable

---

## Related Issues & Future Work

- **mirante.md**: "CTE projection problem" - WILL BE FIXED by this plan
- **testv2.sql Query #7**: WILL PASS after implementation
- **Subquery aliases**: Similar pattern, consider in separate feature
- **Derived table aliases**: Related pattern, separate investigation

---

## Next Steps

1. **Review this plan** with team
2. **Start Phase 1**: Extend SelectQueryModel and QueryCteSpec
3. **Trace one query** through the fix to validate assumptions
4. **Implement Phases 1-3** (core fix)
5. **Add Tests** (Phases 6.1-6.3)
6. **Run E2E** (Phase 6.4 with testv2.sql)
7. **Final QA**: Pre-commit checklist
