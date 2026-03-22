# SILK ROAD: Predicate Algebra — From Raw SQL to Structured Types

## Executive Summary

**Problem:** Worker re-parses raw SQL filter strings, causing fragile string-matching bugs, lack of type safety, and operator extensibility issues.

**Solution:** Refactor to structured PredicateExpr objects—server translates SQL Expr → typed PredicateExpr, worker executes structured objects without re-parsing.

**Impact:** 
- Eliminates BETWEEN+IN combination bugs (Statement 25)
- Eliminates != operator detection mysteries (Statements 12, 18)
- Type-safe filter execution
- Clean operator extensibility path

**Effort:** 13–18 hours across 5 phases

**Phase Breakdown:**
- **Phase 1:** Protobuf message design (2–3h)
- **Phase 2:** Expr → PredicateExpr translation (4–5h)
- **Phase 3:** PredicateExpr → boolean mask execution (3–4h)
- **Phase 4:** Integration & migration (2–3h)
- **Phase 5:** Comprehensive testing (2–3h)

---

## Documents

### Discovery
📄 [discovery-predicate-algebra-phase1.md](discovery/discovery-predicate-algebra-phase1.md)

**Covers:**
- ✅ Current architecture (raw SQL strings via protobuf)
- ✅ Future architecture (structured PredicateExpr)
- ✅ Design gaps & blockers (proto messages, server translation, worker execution, type coercion, backward compat)
- ✅ High-level breakdown into 5 plans
- ✅ Open questions
- ✅ Evidence & references linking to current bugs

**Read this to understand:** Why we're doing this, what's broken, what the target state looks like.

### Implementation Plan
📄 [plans/plan-predicate-algebra-phase1.md](plans/plan-predicate-algebra-phase1.md)

**Covers:**
- ✅ Detailed steps for each of 5 phases (proto, translator, executor, integration, testing)
- ✅ File changes (created, modified)
- ✅ Verification steps per phase
- ✅ Sequencing & parallelization strategy
- ✅ Success criteria
- ✅ Open decisions with recommendations

**Read this to understand:** How to implement each phase, what to write, how to test.

---

## Quick Navigation

### For Planning
1. Start with discovery → understand the problem space
2. Review plan → understand implementation approach
3. Review sequence → decide start date and team allocation

### For Implementation (Once Approved)
1. Phase 1: Follow steps in plan-predicate-algebra-phase1.md → Phase 1: Protobuf & Message Design
2. Phase 2: Follow steps → Phase 2: Server-Side Translation
3. (Phases 3–5 can start immediately after Phase 1 is merged if team parallelizes)

### For Review
1. Check discovery → is the problem correctly characterized?
2. Check plan → are the steps feasible? Any missing steps?
3. Check effort estimates → are 2–3 hours per phase realistic given team velocity?

---

## Key Constraints & Assumptions

1. **No Breaking Changes During Migration:** Old systems must still work during transition. Plan includes backward compatibility strategy (keep raw SQL path alongside).

2. **Type Safety at Server Time:** Type validation happens when PredicateExpr is built on server, not at worker. Worker can assume all predicates are type-safe.

3. **Scope Limitations:** New operators (LIKE, REGEXP, functions) are deferred to Phase 10+. Current plan covers: =, !=, >, >=, <, <=, BETWEEN, IN, IS NULL, IS NOT NULL, AND, OR, NOT.

4. **Proto Evolution:** Each new operator addition requires proto enum variant. Plan anticipates this but defers versions beyond current scope.

---

## Decision Gate Checklist

Before starting implementation, confirm:

- [ ] Architecture (structured predicates vs. raw SQL re-parsing) is approved
- [ ] Scope (5 operators + logical combinators) is correct
- [ ] Effort estimate (13–18 hours) is acceptable
- [ ] Timing: Can start immediately, or defer to post-Phase 9c?
- [ ] Backward compat strategy (keep raw SQL path for N releases) is acceptable
- [ ] Team has capacity for focused 2.5–3 day sprint

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Protobuf compilation fails | Test proto early (Phase 1); have Cargo build verify message generation |
| Server planner integration complex | Existing schema_metadata threading from Phase 9b is a template; follow same pattern |
| Worker executor doesn't match Phase 2 output | Unit test PredicateExpr deserialization in isolation; co-develop with Phase 2 |
| Regression in existing tests | Phase 4 includes fallback to raw SQL path; comprehensive Phase 5 testing |
| Type coercion rules conflict | Phase 9b rules are canonical; Phase 2 translator uses them directly; no new rules |

---

## Success Metrics (Post-Implementation)

✅ phase9_simple_test.sql: 24/24 statements pass (up from 21/25 current)
✅ No worker string parsing for filters (all via PredicateExpr evaluation)
✅ New operator additions require only proto enum change + executor pattern match (no string parsing logic)
✅ Filter execution latency unchanged or improved (1 parse instead of 2)
✅ Code coverage >90% for new modules

---

## Next Steps

1. **Review & Approval:** Stakeholder review of discovery + plan
2. **Refine Scope:** Any changes to operator list or type system?
3. **Schedule:** Decide start date (immediately post-Phase 9c or concurrent?)
4. **Team Assignment:** Who owns each phase? 
5. **Kick-off:** Create issue/task tracker with 5 phase tasks linked

---

## Related Roadmap Items

- **Phase 9c (Current):** BETWEEN/IN predicate support (workaround via string parsing)
- **Phase 9b (Complete):** Type coercion foundation (rules codified, available for reuse)
- **Phase 10+ (Future):** Scalar functions, expression optimization
- This SILK ROAD Path: Structured Predicates (architectural foundation for clean operator extensibility)

---

## Documentation Locations

| Document | Purpose | Location |
|----------|---------|----------|
| Discovery | What & Why | [discovery/discovery-predicate-algebra-phase1.md](discovery/discovery-predicate-algebra-phase1.md) |
| Implementation Plan | How | [plans/plan-predicate-algebra-phase1.md](plans/plan-predicate-algebra-phase1.md) |
| This index | Navigation & decisions | [README.md](README.md) (this file) |

---

**Author's Note:** This SILK ROAD path represents a major architectural improvement that will pay dividends as we add more filter operators and express more complex queries. The Phase 1 discovery uncovered not just bugs, but a systemic architectural issue. Fixing it now creates a foundation for clean, type-safe query execution in all future phases.
