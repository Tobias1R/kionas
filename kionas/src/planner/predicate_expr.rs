use serde::{Deserialize, Serialize};

/// What: Comparison operators supported by structured filter predicates.
///
/// Inputs:
/// - Operator variant selected during predicate translation.
///
/// Output:
/// - Typed operator tag consumed by worker execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PredicateComparisonOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// What: Scalar and homogeneous-list literal values used by predicates.
///
/// Inputs:
/// - Raw literal text parsed from SQL predicate clauses.
///
/// Output:
/// - Typed literal value preserved for worker-side execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PredicateValue {
    Int(i64),
    Bool(bool),
    Str(String),
    IntList(Vec<i64>),
    BoolList(Vec<bool>),
    StrList(Vec<String>),
}

/// What: Structured predicate expression emitted by planner and consumed by worker runtime.
///
/// Inputs:
/// - Predicate-specific fields vary by variant.
///
/// Output:
/// - Serializable typed predicate tree without raw SQL parsing at worker boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PredicateExpr {
    And {
        clauses: Vec<PredicateExpr>,
    },
    Or {
        clauses: Vec<PredicateExpr>,
    },
    Not {
        expr: Box<PredicateExpr>,
    },
    // Deprecated alias preserved for backward compatibility with existing payloads.
    Conjunction {
        clauses: Vec<PredicateExpr>,
    },
    Comparison {
        column: String,
        op: PredicateComparisonOp,
        value: PredicateValue,
    },
    Between {
        column: String,
        lower: PredicateValue,
        upper: PredicateValue,
        negated: bool,
    },
    InList {
        column: String,
        values: PredicateValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
}

fn parse_filter_value(raw: &str) -> Result<PredicateValue, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(PredicateValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(PredicateValue::Bool(false));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(PredicateValue::Int(value));
    }

    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = trimmed[1..trimmed.len() - 1].trim().to_string();
        return Ok(PredicateValue::Str(inner));
    }

    Err(format!(
        "unsupported filter literal '{}': expected int, bool, or quoted string",
        raw
    ))
}

fn is_word_boundary_at(input: &str, idx: usize) -> bool {
    if idx >= input.len() {
        return true;
    }
    !input.as_bytes()[idx].is_ascii_alphanumeric() && input.as_bytes()[idx] != b'_'
}

fn has_word_at(lower: &str, idx: usize, token: &str) -> bool {
    if idx + token.len() > lower.len() {
        return false;
    }
    if &lower[idx..idx + token.len()] != token {
        return false;
    }
    let before_ok = idx == 0 || is_word_boundary_at(lower, idx - 1);
    let after_ok =
        idx + token.len() >= lower.len() || is_word_boundary_at(lower, idx + token.len());
    before_ok && after_ok
}

fn split_top_level_logical(input: &str, keyword: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let lower = input.to_ascii_lowercase();
    let mut start = 0;
    let mut i = 0;
    let mut depth = 0_i32;
    let mut in_single_quote = false;
    let mut between_pending_and = false;

    while i < input.len() {
        let byte = input.as_bytes()[i];
        if byte == b'\'' {
            in_single_quote = !in_single_quote;
            i += 1;
            continue;
        }

        if in_single_quote {
            i += 1;
            continue;
        }

        if byte == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if byte == b')' {
            depth -= 1;
            i += 1;
            continue;
        }

        if depth == 0 {
            if keyword == "and" && has_word_at(&lower, i, "between") {
                between_pending_and = true;
                i += "between".len();
                continue;
            }

            if has_word_at(&lower, i, keyword) {
                if keyword == "and" && between_pending_and {
                    between_pending_and = false;
                    i += keyword.len();
                    continue;
                }

                let part = input[start..i].trim();
                if !part.is_empty() {
                    parts.push(part.to_string());
                }
                i += keyword.len();
                start = i;
                continue;
            }
        }

        i += 1;
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_string());
    }

    parts
}

fn is_wrapped_by_outer_parentheses(input: &str) -> bool {
    if !(input.starts_with('(') && input.ends_with(')')) {
        return false;
    }

    let mut depth = 0_i32;
    let mut in_single_quote = false;

    for (idx, byte) in input.as_bytes().iter().enumerate() {
        if *byte == b'\'' {
            in_single_quote = !in_single_quote;
            continue;
        }

        if in_single_quote {
            continue;
        }

        if *byte == b'(' {
            depth += 1;
        } else if *byte == b')' {
            depth -= 1;
            if depth == 0 && idx < input.len() - 1 {
                return false;
            }
        }
    }

    depth == 0
}

fn trim_outer_parentheses(input: &str) -> &str {
    let mut trimmed = input.trim();
    while is_wrapped_by_outer_parentheses(trimmed) {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    trimmed
}

fn parse_predicate_leaf(raw: &str) -> Result<PredicateExpr, String> {
    let lower = format!(" {} ", raw.to_ascii_lowercase());

    if lower.contains(" is not null ") {
        let idx = lower
            .find(" is not null ")
            .ok_or_else(|| format!("invalid IS NOT NULL clause '{}'", raw))?;
        let column = raw[..idx].trim().to_ascii_lowercase();
        return Ok(PredicateExpr::IsNotNull { column });
    }

    if lower.contains(" is null ") {
        let idx = lower
            .find(" is null ")
            .ok_or_else(|| format!("invalid IS NULL clause '{}'", raw))?;
        let column = raw[..idx].trim().to_ascii_lowercase();
        return Ok(PredicateExpr::IsNull { column });
    }

    if lower.contains(" not between ") {
        return parse_between_clause(raw, true);
    }

    if lower.contains(" between ") {
        return parse_between_clause(raw, false);
    }

    if lower.contains(" in ") {
        return parse_in_clause(raw);
    }

    let (column, op, rhs) = parse_single_clause(raw)?;
    Ok(PredicateExpr::Comparison {
        column,
        op,
        value: parse_filter_value(rhs)?,
    })
}

/// What: Parse SQL filter text into structured predicate expressions with operator precedence.
///
/// Inputs:
/// - `input`: Raw SQL predicate subset emitted by query model.
///
/// Output:
/// - Structured predicate tree used by physical planning and worker execution.
///
/// Details:
/// - Operator precedence is `NOT` (highest), then `AND`, then `OR` (lowest).
/// - Parentheses are honored for explicit grouping.
pub fn parse_predicate_recursive(input: &str) -> Result<PredicateExpr, String> {
    let trimmed = trim_outer_parentheses(input);
    if trimmed.is_empty() {
        return Err("filter predicate is empty".to_string());
    }

    let or_parts = split_top_level_logical(trimmed, "or");
    if or_parts.len() > 1 {
        let mut clauses = Vec::with_capacity(or_parts.len());
        for part in or_parts {
            clauses.push(parse_predicate_recursive(&part)?);
        }
        return Ok(PredicateExpr::Or { clauses });
    }

    let and_parts = split_top_level_logical(trimmed, "and");
    if and_parts.len() > 1 {
        let mut clauses = Vec::with_capacity(and_parts.len());
        for part in and_parts {
            clauses.push(parse_predicate_recursive(&part)?);
        }
        return Ok(PredicateExpr::And { clauses });
    }

    let lower = trimmed.to_ascii_lowercase();
    if has_word_at(&lower, 0, "not") {
        let remainder = trimmed[3..].trim();
        if remainder.is_empty() {
            return Err("NOT predicate is missing expression".to_string());
        }
        return Ok(PredicateExpr::Not {
            expr: Box::new(parse_predicate_recursive(remainder)?),
        });
    }

    parse_predicate_leaf(trimmed)
}

fn parse_single_clause(input: &str) -> Result<(String, PredicateComparisonOp, &str), String> {
    const OPS: [(&str, PredicateComparisonOp); 6] = [
        ("!=", PredicateComparisonOp::Ne),
        (">=", PredicateComparisonOp::Ge),
        ("<=", PredicateComparisonOp::Le),
        ("=", PredicateComparisonOp::Eq),
        (">", PredicateComparisonOp::Gt),
        ("<", PredicateComparisonOp::Lt),
    ];

    for (token, op) in OPS {
        if let Some((lhs, rhs)) = input.split_once(token) {
            let column = lhs.trim().to_ascii_lowercase();
            let literal = rhs.trim();
            if column.is_empty() || literal.is_empty() {
                return Err(format!("invalid filter clause '{}'", input));
            }
            return Ok((column, op, literal));
        }
    }

    Err(format!(
        "unsupported filter clause '{}': expected one of =, !=, >, >=, <, <=",
        input
    ))
}

fn parse_between_clause(clause: &str, negated: bool) -> Result<PredicateExpr, String> {
    let lower_case = clause.to_ascii_lowercase();
    if let Some(between_idx) = lower_case.find(" between ") {
        let column = clause[..between_idx].trim().to_ascii_lowercase();
        let after_between_idx = between_idx + " between ".len();
        let after_between = &clause[after_between_idx..];
        let after_between_lower = after_between.to_ascii_lowercase();
        if let Some(and_idx) = after_between_lower.find(" and ") {
            let lower_part = after_between[..and_idx].trim();
            let upper_part = after_between[and_idx + " and ".len()..].trim();
            return Ok(PredicateExpr::Between {
                column,
                lower: parse_filter_value(lower_part)?,
                upper: parse_filter_value(upper_part)?,
                negated,
            });
        }
    }

    Err(format!(
        "invalid BETWEEN clause '{}': expected 'column BETWEEN lower AND upper'",
        clause
    ))
}

fn parse_in_clause(clause: &str) -> Result<PredicateExpr, String> {
    let lower = clause.to_ascii_lowercase();
    let in_idx = lower
        .find(" in ")
        .ok_or_else(|| format!("invalid IN clause '{}'", clause))?;
    let column = clause[..in_idx].trim().to_ascii_lowercase();

    let list_raw = clause[in_idx + " in ".len()..].trim();
    if !(list_raw.starts_with('(') && list_raw.ends_with(')')) {
        return Err(format!(
            "invalid IN clause '{}': expected parentheses around value list",
            clause
        ));
    }

    let content = &list_raw[1..list_raw.len() - 1];
    let tokens = content
        .split(',')
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return Err(format!(
            "invalid IN clause '{}': value list is empty",
            clause
        ));
    }

    let mut values = Vec::with_capacity(tokens.len());
    for token in tokens {
        values.push(parse_filter_value(token)?);
    }

    let list_value = match &values[0] {
        PredicateValue::Int(_) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if let PredicateValue::Int(i) = value {
                    out.push(i);
                } else {
                    return Err(format!(
                        "invalid IN clause '{}': mixed literal types are not supported",
                        clause
                    ));
                }
            }
            PredicateValue::IntList(out)
        }
        PredicateValue::Bool(_) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if let PredicateValue::Bool(b) = value {
                    out.push(b);
                } else {
                    return Err(format!(
                        "invalid IN clause '{}': mixed literal types are not supported",
                        clause
                    ));
                }
            }
            PredicateValue::BoolList(out)
        }
        PredicateValue::Str(_) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if let PredicateValue::Str(s) = value {
                    out.push(s);
                } else {
                    return Err(format!(
                        "invalid IN clause '{}': mixed literal types are not supported",
                        clause
                    ));
                }
            }
            PredicateValue::StrList(out)
        }
        PredicateValue::IntList(_) | PredicateValue::BoolList(_) | PredicateValue::StrList(_) => {
            return Err(format!(
                "invalid IN clause '{}': nested lists are not supported",
                clause
            ));
        }
    };

    Ok(PredicateExpr::InList {
        column,
        values: list_value,
    })
}

/// What: Parse SQL filter text into structured predicate expressions.
///
/// Inputs:
/// - `filter_sql`: Raw SQL predicate subset emitted by query model.
///
/// Output:
/// - Structured predicate tree used by physical planning and worker execution.
pub fn parse_predicate_sql(filter_sql: &str) -> Result<PredicateExpr, String> {
    parse_predicate_recursive(filter_sql)
}

/// What: Render a structured predicate into a deterministic diagnostic string.
///
/// Inputs:
/// - `predicate`: Structured predicate expression.
///
/// Output:
/// - Human-readable predicate string for explain and logs.
pub fn render_predicate_expr(predicate: &PredicateExpr) -> String {
    match predicate {
        PredicateExpr::And { clauses } | PredicateExpr::Conjunction { clauses } => clauses
            .iter()
            .map(render_predicate_expr)
            .collect::<Vec<_>>()
            .join(" AND "),
        PredicateExpr::Or { clauses } => clauses
            .iter()
            .map(render_predicate_expr)
            .collect::<Vec<_>>()
            .join(" OR "),
        PredicateExpr::Not { expr } => format!("NOT ({})", render_predicate_expr(expr)),
        PredicateExpr::Comparison { column, op, value } => {
            let op_text = match op {
                PredicateComparisonOp::Eq => "=",
                PredicateComparisonOp::Ne => "!=",
                PredicateComparisonOp::Gt => ">",
                PredicateComparisonOp::Ge => ">=",
                PredicateComparisonOp::Lt => "<",
                PredicateComparisonOp::Le => "<=",
            };
            format!("{} {} {}", column, op_text, render_predicate_value(value))
        }
        PredicateExpr::Between {
            column,
            lower,
            upper,
            negated,
        } => {
            let between_kw = if *negated { "NOT BETWEEN" } else { "BETWEEN" };
            format!(
                "{} {} {} AND {}",
                column,
                between_kw,
                render_predicate_value(lower),
                render_predicate_value(upper)
            )
        }
        PredicateExpr::InList { column, values } => {
            format!("{} IN {}", column, render_predicate_value(values))
        }
        PredicateExpr::IsNull { column } => format!("{} IS NULL", column),
        PredicateExpr::IsNotNull { column } => format!("{} IS NOT NULL", column),
    }
}

fn render_predicate_value(value: &PredicateValue) -> String {
    match value {
        PredicateValue::Int(v) => v.to_string(),
        PredicateValue::Bool(v) => v.to_string(),
        PredicateValue::Str(v) => format!("'{}'", v),
        PredicateValue::IntList(values) => format!(
            "({})",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PredicateValue::BoolList(values) => format!(
            "({})",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PredicateValue::StrList(values) => format!(
            "({})",
            values
                .iter()
                .map(|v| format!("'{}'", v))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PredicateComparisonOp, PredicateExpr, PredicateValue, parse_predicate_sql,
        render_predicate_expr,
    };

    #[test]
    fn parses_single_comparison_predicate() {
        let parsed = parse_predicate_sql("id = 42").expect("comparison predicate must parse");
        assert_eq!(
            parsed,
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(42),
            }
        );
    }

    #[test]
    fn parses_conjunction_with_between_and_in() {
        let parsed = parse_predicate_sql("id BETWEEN 1 AND 10 AND name IN ('alice', 'bob')")
            .expect("compound predicate must parse");

        match parsed {
            PredicateExpr::And { clauses } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(clauses[0], PredicateExpr::Between { .. }));
                assert!(matches!(clauses[1], PredicateExpr::InList { .. }));
            }
            _ => panic!("expected conjunction predicate"),
        }
    }

    #[test]
    fn parses_not_between_as_negated_between() {
        let parsed = parse_predicate_sql("score NOT BETWEEN 5 AND 7")
            .expect("NOT BETWEEN predicate must parse");

        assert_eq!(
            parsed,
            PredicateExpr::Between {
                column: "score".to_string(),
                lower: PredicateValue::Int(5),
                upper: PredicateValue::Int(7),
                negated: true,
            }
        );
    }

    #[test]
    fn parses_null_checks() {
        let parsed = parse_predicate_sql("name IS NOT NULL AND deleted_at IS NULL")
            .expect("null-check predicates must parse");

        match parsed {
            PredicateExpr::And { clauses } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(clauses[0], PredicateExpr::IsNotNull { .. }));
                assert!(matches!(clauses[1], PredicateExpr::IsNull { .. }));
            }
            _ => panic!("expected conjunction predicate"),
        }
    }

    #[test]
    fn rejects_mixed_in_list_types() {
        let err = parse_predicate_sql("id IN (1, 'two')")
            .expect_err("mixed literal types in IN must be rejected");
        assert!(err.contains("mixed literal types"));
    }

    #[test]
    fn renders_conjunction_expression() {
        let predicate = PredicateExpr::And {
            clauses: vec![
                PredicateExpr::Comparison {
                    column: "id".to_string(),
                    op: PredicateComparisonOp::Ge,
                    value: PredicateValue::Int(10),
                },
                PredicateExpr::InList {
                    column: "name".to_string(),
                    values: PredicateValue::StrList(vec!["alice".to_string(), "bob".to_string()]),
                },
            ],
        };

        let rendered = render_predicate_expr(&predicate);
        assert_eq!(rendered, "id >= 10 AND name IN ('alice', 'bob')");
    }

    #[test]
    fn parses_not_and_or_with_precedence() {
        let parsed = parse_predicate_sql("a = 1 OR b = 2 AND NOT c = 3")
            .expect("predicate with OR/AND/NOT must parse");

        match parsed {
            PredicateExpr::Or { clauses } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(clauses[0], PredicateExpr::Comparison { .. }));
                match &clauses[1] {
                    PredicateExpr::And { clauses } => {
                        assert_eq!(clauses.len(), 2);
                        assert!(matches!(clauses[0], PredicateExpr::Comparison { .. }));
                        assert!(matches!(clauses[1], PredicateExpr::Not { .. }));
                    }
                    _ => panic!("expected right-hand side AND clause"),
                }
            }
            _ => panic!("expected OR predicate"),
        }
    }
}
