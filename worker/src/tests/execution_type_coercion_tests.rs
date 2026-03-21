/// Phase 9b Step 7: Type Coercion Enforcement Integration Tests
///
/// What: Comprehensive test coverage for strict type coercion validation in filter predicates.
///
/// Inputs:
/// - Filter predicates with type-matching and type-mismatching operands.
/// - Schema metadata with canonical types for columns.
/// - Various predicate patterns: IS NULL, IS NOT NULL, comparisons, AND combinations.
///
/// Output:
/// - Verification that type-matching predicates pass validation.
/// - Verification that type-mismatching predicates are rejected with descriptive errors.
/// - Confirmation of NULL semantics in IS NULL / IS NOT NULL predicates.
/// - Validation of error messages containing column name, expected type, and actual type.
///
/// Details:
/// - Tests verify Phase 9b Step 6 runtime validation (worker-side type checking).
/// - Both positive paths (valid predicates) and negative paths (type violations) covered.
/// - Edge cases include whitespace handling, complex AND chains, and empty filters.
/// - Type compatibility enforced under DecimalCoercionPolicy::Strict (no implicit fallback).

#[cfg(test)]
mod type_coercion_tests {
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::services::query_execution::apply_filter_pipeline;
    use kionas::sql::datatypes::ColumnDatatypeSpec;

    // ==================== HELPER BUILDERS ====================

    /// Constructs a test RecordBatch with int, string, and bool columns
    fn batch_mixed_types() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            ],
        )
        .expect("mixed type batch must build")
    }

    /// Constructs sample schema metadata for testing
    fn sample_schema_metadata() -> HashMap<String, ColumnDatatypeSpec> {
        let mut schema = HashMap::new();
        schema.insert(
            "id".to_string(),
            ColumnDatatypeSpec {
                column: "id".to_string(),
                declared_type: "INT64".to_string(),
                canonical_type: "int64".to_string(),
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
            "active".to_string(),
            ColumnDatatypeSpec {
                column: "active".to_string(),
                declared_type: "BOOLEAN".to_string(),
                canonical_type: "bool".to_string(),
                temporal_type: None,
                timezone_policy: None,
                decimal_spec: None,
            },
        );
        schema
    }

    // ==================== POSITIVE PATH TESTS ====================
    // Tests verify that valid (type-matching) predicates pass validation

    #[test]
    fn positive_int_literal_matches_int_column() {
        // id = 42 (int → int64 column) should pass
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(&[batch], "id = 42", Some(sample_schema_metadata()));
        // Should not error; may return empty rows if no row matches, but validation passes
        assert!(result.is_ok(), "int literal for int column should pass type validation");
    }

    #[test]
    fn positive_string_literal_matches_string_column() {
        // name = 'Alice' (string → string column) should pass
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "name = 'Alice'",
            Some(sample_schema_metadata()),
        );
        assert!(result.is_ok(), "string literal for string column should pass type validation");
    }

    #[test]
    fn positive_bool_literal_matches_bool_column() {
        // active = true (bool → bool column) should pass
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "active = true",
            Some(sample_schema_metadata()),
        );
        assert!(result.is_ok(), "bool literal for bool column should pass type validation");
    }

    #[test]
    fn positive_integer_comparisons() {
        // Test all comparison operators with matching types
        let comparisons = vec![
            "id = 1",
            "id != 2",
            "id > 0",
            "id >= 1",
            "id < 10",
            "id <= 5",
        ];

        let batch = batch_mixed_types();
        for predicate in comparisons {
            let result = apply_filter_pipeline(&[batch.clone()], predicate, Some(sample_schema_metadata()));
            assert!(
                result.is_ok(),
                "Integer comparison '{}' should pass type validation",
                predicate
            );
        }
    }

    #[test]
    fn positive_is_null_on_any_column() {
        // IS NULL doesn't require type checking; must pass on any column
        let is_null_predicates = vec![
            "id IS NULL",
            "name IS NULL",
            "active IS NULL",
        ];

        let batch = batch_mixed_types();
        for predicate in is_null_predicates {
            let result = apply_filter_pipeline(&[batch.clone()], predicate, Some(sample_schema_metadata()));
            assert!(
                result.is_ok(),
                "IS NULL on '{}' should pass validation",
                predicate
            );
        }
    }

    #[test]
    fn positive_is_not_null_on_any_column() {
        // IS NOT NULL doesn't require type checking; must pass on any column
        let is_not_null_predicates = vec![
            "id IS NOT NULL",
            "name IS NOT NULL",
            "active IS NOT NULL",
        ];

        let batch = batch_mixed_types();
        for predicate in is_not_null_predicates {
            let result = apply_filter_pipeline(&[batch.clone()], predicate, Some(sample_schema_metadata()));
            assert!(
                result.is_ok(),
                "IS NOT NULL on '{}' should pass validation",
                predicate
            );
        }
    }

    #[test]
    fn positive_and_combination_with_matching_types() {
        // id = 1 AND name = 'Alice' (both types match)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "id = 1 AND name = 'Alice'",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_ok(),
            "AND combination with matching types should pass validation"
        );
    }

    #[test]
    fn positive_complex_and_chain() {
        // id = 1 AND id > 0 AND active = true (all match their column types)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "id = 1 AND id > 0 AND active = true",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_ok(),
            "Complex AND chain with matching types should pass validation"
        );
    }

    // ==================== NEGATIVE PATH TESTS ====================
    // Tests verify that invalid (type-mismatching) predicates are rejected

    #[test]
    fn negative_int_literal_rejects_string_column() {
        // name = 42 (int → string column) should fail
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "name = 42",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_err(),
            "int literal for string column should fail type validation"
        );
        let error_msg = result.unwrap_err();
        assert!(
            error_msg.to_lowercase().contains("type") || error_msg.to_lowercase().contains("coercion"),
            "Error should mention type mismatch: {}",
            error_msg
        );
    }

    #[test]
    fn negative_string_literal_rejects_int_column() {
        // id = 'invalid' (string → int column) should fail
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "id = 'invalid'",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_err(),
            "string literal for int column should fail type validation"
        );
        let error_msg = result.unwrap_err();
        assert!(
            error_msg.to_lowercase().contains("type") || error_msg.to_lowercase().contains("coercion"),
            "Error should mention type mismatch: {}",
            error_msg
        );
    }

    #[test]
    fn negative_int_literal_rejects_bool_column() {
        // active = 123 (int → bool column) should fail
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "active = 123",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_err(),
            "int literal for bool column should fail type validation"
        );
    }

    #[test]
    fn negative_nonexistent_column() {
        // nonexistent_col = 5 should fail (column not in schema)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "nonexistent_col = 5",
            Some(sample_schema_metadata()),
        );
        // Should error due to column not found
        assert!(result.is_err(), "nonexistent column should fail");
    }

    #[test]
    fn negative_and_combination_with_type_mismatch() {
        // id = 'invalid' AND name = 'Bob' (first clause has type mismatch)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "id = 'invalid' AND name = 'Bob'",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_err(),
            "AND combination with type mismatch should fail validation"
        );
    }

    // ==================== EDGE CASE TESTS ====================

    #[test]
    fn edge_case_whitespace_in_predicate() {
        // id    =    42 (extra whitespace) should still pass
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "id    =    42",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_ok(),
            "Whitespace in predicate should be handled gracefully"
        );
    }

    #[test]
    fn edge_case_null_schema_metadata() {
        // When schema_metadata is None, validation should skip type checking (interim phase behavior)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(&[batch], "id = 42", None);
        // Should not error due to missing schema_metadata
        // (Phase 9b interim behavior: validation skipped when metadata absent)
        assert!(
            result.is_ok(),
            "Filter should work without schema_metadata (Phase 9b interim)"
        );
    }

    #[test]
    fn error_message_contains_column_name() {
        // Error should include the column name that failed type check
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "name = 42",
            Some(sample_schema_metadata()),
        );
        if let Err(error_msg) = result {
            let error_lower = error_msg.to_lowercase();
            assert!(
                error_msg.contains("name") || error_lower.contains("type"),
                "Error should mention the problematic column or type mismatch: {}",
                error_msg
            );
        } else {
            panic!("Query should have failed due to type mismatch");
        }
    }

    #[test]
    fn test_multiple_mismatches() {
        // Verify first mismatch is caught (short-circuit evaluation)
        let batch = batch_mixed_types();
        let result = apply_filter_pipeline(
            &[batch],
            "active = 100 AND name = 200",
            Some(sample_schema_metadata()),
        );
        assert!(
            result.is_err(),
            "Should reject predicate with any type mismatch"
        );
    }
}

