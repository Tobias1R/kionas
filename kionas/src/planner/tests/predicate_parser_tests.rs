use crate::planner::{PredicateExpr, parse_predicate_recursive};

#[test]
fn parses_simple_and_expression() {
    let parsed = parse_predicate_recursive("a = 1 AND b = 2").expect("AND predicate must parse");
    match parsed {
        PredicateExpr::And { clauses } => {
            assert_eq!(clauses.len(), 2);
            assert!(matches!(clauses[0], PredicateExpr::Comparison { .. }));
            assert!(matches!(clauses[1], PredicateExpr::Comparison { .. }));
        }
        _ => panic!("expected AND predicate"),
    }
}

#[test]
fn parses_simple_or_expression() {
    let parsed = parse_predicate_recursive("a = 1 OR b = 2").expect("OR predicate must parse");
    match parsed {
        PredicateExpr::Or { clauses } => {
            assert_eq!(clauses.len(), 2);
            assert!(matches!(clauses[0], PredicateExpr::Comparison { .. }));
            assert!(matches!(clauses[1], PredicateExpr::Comparison { .. }));
        }
        _ => panic!("expected OR predicate"),
    }
}

#[test]
fn parses_not_expression() {
    let parsed = parse_predicate_recursive("NOT a = 1").expect("NOT predicate must parse");
    assert!(matches!(parsed, PredicateExpr::Not { .. }));
}

#[test]
fn parses_nested_expression_with_parentheses() {
    let parsed = parse_predicate_recursive("(a = 1 AND b = 2) OR c = 3")
        .expect("nested predicate must parse");
    match parsed {
        PredicateExpr::Or { clauses } => {
            assert_eq!(clauses.len(), 2);
            assert!(matches!(clauses[0], PredicateExpr::And { .. }));
            assert!(matches!(clauses[1], PredicateExpr::Comparison { .. }));
        }
        _ => panic!("expected OR predicate"),
    }
}

#[test]
fn enforces_logical_precedence() {
    let parsed = parse_predicate_recursive("a = 1 OR b = 2 AND c = 3")
        .expect("precedence predicate must parse");
    match parsed {
        PredicateExpr::Or { clauses } => {
            assert_eq!(clauses.len(), 2);
            assert!(matches!(clauses[0], PredicateExpr::Comparison { .. }));
            assert!(matches!(clauses[1], PredicateExpr::And { .. }));
        }
        _ => panic!("expected OR predicate"),
    }
}

#[test]
fn parses_deeply_nested_expression() {
    let parsed = parse_predicate_recursive("((a = 1 AND b = 2) OR (c = 3 AND d = 4)) AND e = 5")
        .expect("deeply nested predicate must parse");
    match parsed {
        PredicateExpr::And { clauses } => {
            assert_eq!(clauses.len(), 2);
            assert!(matches!(clauses[0], PredicateExpr::Or { .. }));
            assert!(matches!(clauses[1], PredicateExpr::Comparison { .. }));
        }
        _ => panic!("expected AND predicate"),
    }
}

#[test]
fn rejects_empty_input() {
    let err = parse_predicate_recursive("   ").expect_err("empty predicate must fail");
    assert!(err.contains("empty"));
}

#[test]
fn rejects_missing_rhs_operand() {
    let err = parse_predicate_recursive("a = 1 AND").expect_err("missing rhs must fail");
    assert!(err.contains("invalid") || err.contains("unsupported") || err.contains("empty"));
}
