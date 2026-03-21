use crate::planner::{PhysicalExpr, PlannerError};
use crate::sql::datatypes::ColumnDatatypeSpec;
use std::collections::HashMap;

/// What: Format planner-side type error message with remediation suggestions.
///
/// Inputs:
/// - `scenario`: Error scenario (MissingColumn, TypeMismatch, UnsupportedPredicate)
/// - `column`: Column name (if applicable)
/// - `column_type`: Expected column type (if applicable)
/// - `predicate`: Original predicate SQL for context
///
/// Output:
/// - Descriptive error message with remediation guidance
///
/// Details:
/// - Phase 9b Step 8 error taxonomy: consistent error messages across planner and worker
/// - Provides actionable remediation suggestions based on error category
fn format_planner_type_error(
    scenario: &str,
    column: &str,
    column_type: Option<&str>,
    predicate: &str,
) -> String {
    match scenario {
        "MissingColumn" => {
            format!(
                "column '{}' not found in table schema for predicate '{}'. \
                remediations: [verify column name spelling, check table schema definition, list available columns]",
                column, predicate
            )
        }
        "TypeMismatch" => {
            format!(
                "predicate '{}' violates strict type coercion policy. \
                column '{}' expects type '{}' but operand type incompatible. \
                remediations: [use CAST() to convert operand, adjust schema definition, rewrite predicate]",
                predicate,
                column,
                column_type.unwrap_or("(unknown)")
            )
        }
        "UnsupportedPredicate" => {
            format!(
                "predicate '{}' uses unsupported syntax or operators in this phase. \
                remediations: [use simple AND combinations, use IS NULL/IS NOT NULL for NULL checks, use basic comparison operators (=, <, >, <=, >=, !=)]",
                predicate
            )
        }
        _ => {
            format!(
                "type coercion violation in predicate '{}': {}",
                predicate, scenario
            )
        }
    }
}

/// What: Validate Filter predicate expressions against column type contracts.
///
/// Inputs:
/// - `predicate`: The filter predicate expression.
/// - `column_types`: Mapping of column names to their datatype specifications.
///
/// Output:
/// - Ok(()) if predicate is type-compatible with column schema.
/// - Err(PlannerError::TypeCoercionViolation) if predicate violates strict type coercion policy.
///
/// Details:
/// - Phase 9b focuses on strict type coercion: operands must match column types exactly.
/// - NULL operands compatible with any column (IS NULL/IS NOT NULL semantic).
/// - Phase 9b Step 8: Enhanced error messages with taxonomy and remediation guidance
/// - Deferred: Implicit coercions (int->decimal), type narrowing/widening.
/// - For Phase 9b, this function logs violations but testing phase accepts both type-valid and
///   type-invalid predicates to establish baseline strictness.
pub fn check_filter_predicate_types(
    predicate: &PhysicalExpr,
    column_types: &HashMap<String, ColumnDatatypeSpec>,
) -> Result<(), PlannerError> {
    match predicate {
        PhysicalExpr::ColumnRef { name } => {
            // Column references in predicates are valid if column exists in schema.
            // Final validation deferred to Phase 9b Step 6 (worker runtime).
            if !column_types.contains_key(name) {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("MissingColumn", name, None, name),
                ));
            }
            Ok(())
        }
        PhysicalExpr::Raw { sql } => {
            // Raw SQL expressions: validate structure for simple IS NULL/IS NOT NULL patterns.
            // For Phase 9b Step 5, log known patterns; comprehensive type checking deferred to Step 6.
            validate_raw_sql_predicate(sql, column_types)
        }
    }
}

/// What: Validate raw SQL predicate statements for type compatibility.
///
/// Inputs:
/// - `sql`: Raw SQL predicate string.
/// - `column_types`: Column schema mapping.
///
/// Output:
/// - Ok(()) for recognized patterns; errors for conclusively invalid patterns.
///
/// Details:
/// - Phase 9b Step 5 focuses on parsing predicates to extract column references.
/// - Phase 9b Step 8: Enhanced error messages with scenario-specific remediation
/// - Deferred to Step 6: Full expression tree type checking.
fn validate_raw_sql_predicate(
    sql: &str,
    column_types: &HashMap<String, ColumnDatatypeSpec>,
) -> Result<(), PlannerError> {
    let trimmed = sql.trim().to_ascii_lowercase();

    // Phase 9a pattern: IS NULL / IS NOT NULL (already supported)
    if trimmed.ends_with("is null") || trimmed.ends_with("is not null") {
        // Extract column name from pattern: "column IS NULL" or "column IS NOT NULL"
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if !parts.is_empty() {
            let column_name = parts[0];
            if !column_types.contains_key(column_name) {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("MissingColumn", column_name, None, sql),
                ));
            }
        }
        return Ok(());
    }

    // Phase 9b pattern: NOT (IS NULL) / NOT (IS NOT NULL)
    if trimmed.starts_with("not")
        && (trimmed.contains("is null") || trimmed.contains("is not null"))
    {
        // Extract column name from pattern: "NOT (column IS NULL)"
        let inner = trimmed.trim_start_matches("not").trim();
        if inner.starts_with('(') && inner.ends_with(')') {
            let content = &inner[1..inner.len() - 1];
            let parts: Vec<&str> = content.split_whitespace().collect();
            if !parts.is_empty() {
                let column_name = parts[0];
                if !column_types.contains_key(column_name) {
                    return Err(PlannerError::TypeCoercionViolation(
                        format_planner_type_error("MissingColumn", column_name, None, sql),
                    ));
                }
            }
        }
        return Ok(());
    }

    // Phase 9b pattern: Comparison operators (=, <, >, <=, >=, !=)
    // Phase 9b Step 5 recognizes pattern but defers type checking to Step 6
    if let Some(column_name) = extract_first_operand(&trimmed)
        && !column_types.contains_key(&column_name)
    {
        return Err(PlannerError::TypeCoercionViolation(
            format_planner_type_error("MissingColumn", &column_name, None, sql),
        ));
    }

    // Phase 9c pattern: BETWEEN "column BETWEEN lower AND upper"
    if trimmed.contains(" between ") {
        return validate_between_predicate(sql, column_types);
    }

    // Phase 9c pattern: IN "column IN (val1, val2, ...)"
    if trimmed.contains(" in (") {
        return validate_in_predicate(sql, column_types);
    }

    Ok(())
}

/// What: Extract the first operand (column name) from a predicate SQL string.
///
/// Inputs:
/// - `sql`: Lowercased predicate SQL.
///
/// Output:
/// - Some(column_name) if first token looks like a column name.
/// - None if predicate structure unrecognized.
fn extract_first_operand(sql: &str) -> Option<String> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let first = parts[0];
    // Skip non-column tokens (operators, functions, etc.)
    if first.starts_with('(') || first.contains('(') || first.contains(',') {
        return None;
    }

    Some(first.to_string())
}

/// What: Infer the type of a filter literal value (bool, int, or string).
///
/// Inputs:
/// - `literal`: Raw literal value from predicate.
///
/// Output:
/// - Some("bool") for true/false
/// - Some("int") for numeric values
/// - Some("string") for quoted strings
/// - None for unrecognized formats
fn infer_operand_type(literal: &str) -> Option<String> {
    let trimmed = literal.trim();

    // Check for boolean literals (true/false, case-insensitive)
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return Some("bool".to_string());
    }

    // Check for string literals (quoted with single quotes)
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return Some("string".to_string());
    }

    // Check for numeric literals
    if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
        return Some("int".to_string());
    }

    None
}

/// What: Validate BETWEEN predicate: "column BETWEEN lower AND upper".
///
/// Inputs:
/// - `sql`: Raw BETWEEN predicate string.
/// - `column_types`: Column schema mapping.
///
/// Output:
/// - Ok(()) if BETWEEN pattern is valid and types are compatible.
/// - Err(PlannerError::TypeCoercionViolation) for structural or type errors.
fn validate_between_predicate(
    sql: &str,
    column_types: &HashMap<String, ColumnDatatypeSpec>,
) -> Result<(), PlannerError> {
    let trimmed = sql.trim();
    // Pattern: "column BETWEEN lower AND upper"
    let lower_case = trimmed.to_ascii_lowercase();

    // Locate BETWEEN keyword
    if let Some(between_pos) = lower_case.find(" between ") {
        let column_part = trimmed[..between_pos].trim();
        let after_between = trimmed[between_pos + 9..].trim();

        // Find AND keyword that separates lower and upper bounds
        if let Some(and_pos) = after_between.to_ascii_lowercase().find(" and ") {
            let lower_part = after_between[..and_pos].trim();
            let upper_part = after_between[and_pos + 5..].trim();

            // Validate column exists
            if !column_types.contains_key(column_part) {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("MissingColumn", column_part, None, sql),
                ));
            }

            // Infer types of bounds
            let lower_type = infer_operand_type(lower_part);
            let upper_type = infer_operand_type(upper_part);

            // Both bounds must have inferable types
            if lower_type.is_none() || upper_type.is_none() {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("UnsupportedPredicate", column_part, None, sql),
                ));
            }

            // Both bounds must have same type (homogeneity check)
            if lower_type != upper_type {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("TypeMismatch", column_part, None, sql),
                ));
            }

            return Ok(());
        }
    }

    // Pattern not recognized
    Err(PlannerError::TypeCoercionViolation(
        format_planner_type_error("UnsupportedPredicate", "", None, sql),
    ))
}

/// What: Validate IN predicate: "column IN (val1, val2, ...)".
///
/// Inputs:
/// - `sql`: Raw IN predicate string.
/// - `column_types`: Column schema mapping.
///
/// Output:
/// - Ok(()) if IN pattern is valid and all values are type-compatible.
/// - Err(PlannerError::TypeCoercionViolation) for structural or type errors.
fn validate_in_predicate(
    sql: &str,
    column_types: &HashMap<String, ColumnDatatypeSpec>,
) -> Result<(), PlannerError> {
    let trimmed = sql.trim();
    let lower_case = trimmed.to_ascii_lowercase();

    // Pattern: "column IN (val1, val2, ...)"
    if let Some(in_pos) = lower_case.find(" in (") {
        let column_part = trimmed[..in_pos].trim();
        let list_start = in_pos + 5; // " in (" is 5 chars
        let after_in = &trimmed[list_start..];

        // Find closing parenthesis
        if let Some(close_paren) = after_in.rfind(')') {
            let values_str = &after_in[..close_paren];

            // Validate column exists
            if !column_types.contains_key(column_part) {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("MissingColumn", column_part, None, sql),
                ));
            }

            // Parse comma-separated values
            let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();

            if values.is_empty() {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("UnsupportedPredicate", column_part, None, sql),
                ));
            }

            // Infer type of first value
            let first_type = infer_operand_type(values[0]);
            if first_type.is_none() {
                return Err(PlannerError::TypeCoercionViolation(
                    format_planner_type_error("UnsupportedPredicate", column_part, None, sql),
                ));
            }

            // Ensure all values have same type (homogeneity)
            for value in &values[1..] {
                let value_type = infer_operand_type(value);
                if value_type != first_type {
                    return Err(PlannerError::TypeCoercionViolation(
                        format_planner_type_error("TypeMismatch", column_part, None, sql),
                    ));
                }
            }

            return Ok(());
        }
    }

    // Pattern not recognized
    Err(PlannerError::TypeCoercionViolation(
        format_planner_type_error("UnsupportedPredicate", "", None, sql),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::datatypes::{DecimalSpec, TemporalLogicalType, TimezonePolicy};

    fn sample_column_schema() -> HashMap<String, ColumnDatatypeSpec> {
        let mut schema = HashMap::new();
        schema.insert(
            "id".to_string(),
            ColumnDatatypeSpec {
                column: "id".to_string(),
                declared_type: "INT".to_string(),
                canonical_type: "int".to_string(),
                temporal_type: None,
                timezone_policy: None,
                decimal_spec: None,
            },
        );
        schema.insert(
            "name".to_string(),
            ColumnDatatypeSpec {
                column: "name".to_string(),
                declared_type: "VARCHAR(255)".to_string(),
                canonical_type: "string".to_string(),
                temporal_type: None,
                timezone_policy: None,
                decimal_spec: None,
            },
        );
        schema.insert(
            "price".to_string(),
            ColumnDatatypeSpec {
                column: "price".to_string(),
                declared_type: "DECIMAL(10,2)".to_string(),
                canonical_type: "decimal".to_string(),
                temporal_type: None,
                timezone_policy: None,
                decimal_spec: Some(DecimalSpec {
                    precision: 10,
                    scale: 2,
                }),
            },
        );
        schema.insert(
            "created_at".to_string(),
            ColumnDatatypeSpec {
                column: "created_at".to_string(),
                declared_type: "TIMESTAMP".to_string(),
                canonical_type: "timestamp".to_string(),
                temporal_type: Some(TemporalLogicalType::Timestamp),
                timezone_policy: Some(TimezonePolicy::TimezoneAwareUtcNormalized),
                decimal_spec: None,
            },
        );
        schema
    }

    #[test]
    fn accepts_column_ref_when_column_exists() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::ColumnRef {
            name: "id".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn rejects_column_ref_when_column_missing() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::ColumnRef {
            name: "nonexistent".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn accepts_is_null_predicate() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IS NULL".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_is_not_null_predicate() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "name IS NOT NULL".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn rejects_is_null_on_nonexistent_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "nonexistent IS NULL".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn accepts_not_is_null_predicate() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "NOT (id IS NULL)".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_comparison_with_existing_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "price > 100".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn rejects_comparison_with_nonexistent_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "invalid_col = 5".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    #[test]
    fn accepts_timestamp_column_predicate() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "created_at IS NOT NULL".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    // Phase 9c BETWEEN tests: "column BETWEEN lower AND upper"

    #[test]
    fn accepts_between_with_integer_bounds() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id BETWEEN 1 AND 100".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_between_with_string_bounds() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "name BETWEEN 'A' AND 'Z'".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn rejects_between_with_missing_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "nonexistent BETWEEN 1 AND 100".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn rejects_between_with_mismatched_bound_types() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id BETWEEN 1 AND 'string'".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    #[test]
    fn rejects_between_with_invalid_lower_bound() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id BETWEEN invalid_literal AND 100".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    // Phase 9c IN tests: "column IN (val1, val2, ...)"

    #[test]
    fn accepts_in_with_integer_list() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN (1, 2, 3, 4, 5)".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_in_with_string_list() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "name IN ('alice', 'bob', 'charlie')".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_in_with_single_value() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN (42)".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn rejects_in_with_missing_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "nonexistent IN (1, 2, 3)".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn rejects_in_with_heterogeneous_types() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN (1, 'two', 3)".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    #[test]
    fn rejects_in_with_empty_list() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN ()".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    #[test]
    fn rejects_in_with_invalid_literals() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN (a, b, c)".to_string(),
        };
        let err = check_filter_predicate_types(&expr, &schema).unwrap_err();
        assert!(matches!(err, PlannerError::TypeCoercionViolation(_)));
    }

    #[test]
    fn accepts_between_with_case_insensitive_keyword() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id between 10 and 50".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_in_with_case_insensitive_keyword() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id in (1, 2, 3)".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_between_with_decimal_column() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "price BETWEEN 10 AND 100".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }

    #[test]
    fn accepts_in_with_multiple_spaces_in_list() {
        let schema = sample_column_schema();
        let expr = PhysicalExpr::Raw {
            sql: "id IN ( 1 , 2 , 3 )".to_string(),
        };
        assert!(check_filter_predicate_types(&expr, &schema).is_ok());
    }
}
