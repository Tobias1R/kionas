use crate::parser::datafusion_sql::sqlparser::ast::{
    Query as SqlQuery, Select, SetExpr, TableFactor,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// What: Canonical payload version used by current query dispatch contract.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Integer payload version included in serialized query payloads.
///
/// Details:
/// - This constant is centralized so server and planner migration can switch versions safely.
pub const QUERY_PAYLOAD_VERSION: u8 = 2;

/// What: Validation outcome code for unsupported query shape errors.
pub const VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE: &str = "UNSUPPORTED_QUERY_SHAPE";
/// What: Validation outcome code for invalid physical pipeline errors.
pub const VALIDATION_CODE_UNSUPPORTED_PIPELINE: &str = "UNSUPPORTED_PIPELINE";
/// What: Validation outcome code for unsupported physical operators.
pub const VALIDATION_CODE_UNSUPPORTED_OPERATOR: &str = "UNSUPPORTED_OPERATOR";
/// What: Validation outcome code for unsupported predicates.
pub const VALIDATION_CODE_UNSUPPORTED_PREDICATE: &str = "UNSUPPORTED_PREDICATE";

/// What: Canonical namespace fields used to identify query source table.
///
/// Inputs:
/// - `database`: Canonical database identifier.
/// - `schema`: Canonical schema identifier.
/// - `table`: Canonical table identifier.
/// - `raw`: Original table name as seen in the SQL AST.
///
/// Output:
/// - Serializable namespace section of the canonical query payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryNamespace {
    pub database: String,
    pub schema: String,
    pub table: String,
    pub raw: String,
}

/// What: Canonical ORDER BY expression shape preserved in query model payload.
///
/// Inputs:
/// - `expression`: Sort key SQL text.
/// - `ascending`: `true` for ASC, `false` for DESC.
///
/// Output:
/// - Deterministic ordering directive consumed by planner translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SortSpec {
    pub expression: String,
    pub ascending: bool,
}

/// What: Shared semantic model for a minimal SELECT query.
///
/// Inputs:
/// - `version`: Canonical payload version.
/// - `statement`: Query statement kind.
/// - `session_id`: Session identifier.
/// - `namespace`: Resolved query namespace metadata.
/// - `projection`: Projection expressions.
/// - `selection`: Optional filter expression.
/// - `order_by`: Optional ORDER BY directives.
/// - `sql`: Canonical SQL text representation.
///
/// Output:
/// - Serializable model consumed by server dispatch and upcoming planner phases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectQueryModel {
    pub version: u8,
    pub statement: String,
    pub session_id: String,
    pub namespace: QueryNamespace,
    pub projection: Vec<String>,
    pub selection: Option<String>,
    pub order_by: Vec<SortSpec>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub sql: String,
}

/// What: Dispatch envelope containing serialized payload and normalized namespace parts.
///
/// Inputs:
/// - `payload`: Canonical JSON payload string.
/// - `database`: Canonical database identifier.
/// - `schema`: Canonical schema identifier.
/// - `table`: Canonical table identifier.
///
/// Output:
/// - Shared transport bundle for server task dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDispatchEnvelope {
    pub payload: String,
    pub database: String,
    pub schema: String,
    pub table: String,
}

/// What: Error type produced while translating SQL AST into shared query model.
///
/// Inputs:
/// - Variants encode specific validation or shape issues found during translation.
///
/// Output:
/// - Typed error that can be surfaced as validation outcome messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryModelError {
    UnsupportedSetOperation,
    UnsupportedMultiTableFrom,
    UnsupportedJoin,
    UnsupportedTableFactor,
    PlannerTranslationFailed(String),
    PlannerPhysicalFailed(String),
    InvalidPhysicalPipeline(String),
    UnsupportedPhysicalOperator(String),
    UnsupportedPredicate(String),
    UnsupportedLimitExpression(String),
    UnsupportedFetchClause,
    OffsetWithoutLimit,
}

impl std::fmt::Display for QueryModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryModelError::UnsupportedSetOperation => write!(
                f,
                "only SELECT queries are supported in this phase (set operations are not supported)"
            ),
            QueryModelError::UnsupportedMultiTableFrom => {
                write!(f, "query must reference exactly one table in FROM")
            }
            QueryModelError::UnsupportedJoin => {
                write!(f, "JOIN is not supported in this phase")
            }
            QueryModelError::UnsupportedTableFactor => write!(
                f,
                "only direct table references are supported in this phase"
            ),
            QueryModelError::PlannerTranslationFailed(message) => {
                write!(f, "failed to build logical plan: {}", message)
            }
            QueryModelError::PlannerPhysicalFailed(message) => {
                write!(f, "failed to build physical plan: {}", message)
            }
            QueryModelError::InvalidPhysicalPipeline(message) => {
                write!(f, "invalid physical pipeline: {}", message)
            }
            QueryModelError::UnsupportedPhysicalOperator(name) => {
                write!(
                    f,
                    "physical operator '{}' is not supported in this phase",
                    name
                )
            }
            QueryModelError::UnsupportedPredicate(name) => {
                write!(f, "predicate is not supported in this phase: {}", name)
            }
            QueryModelError::UnsupportedLimitExpression(message) => {
                write!(f, "unsupported LIMIT/OFFSET expression: {}", message)
            }
            QueryModelError::UnsupportedFetchClause => {
                write!(f, "FETCH clause is not supported in this phase")
            }
            QueryModelError::OffsetWithoutLimit => {
                write!(f, "OFFSET without LIMIT is not supported in this phase")
            }
        }
    }
}

impl std::error::Error for QueryModelError {}

/// What: Map query model errors to stable server validation outcome codes.
///
/// Inputs:
/// - `err`: Query model error from translation or planning.
///
/// Output:
/// - Canonical validation code consumed by server outcome formatting.
pub fn validation_code_for_query_error(err: &QueryModelError) -> &'static str {
    match err {
        QueryModelError::InvalidPhysicalPipeline(_) => VALIDATION_CODE_UNSUPPORTED_PIPELINE,
        QueryModelError::UnsupportedPhysicalOperator(_) => VALIDATION_CODE_UNSUPPORTED_OPERATOR,
        QueryModelError::UnsupportedPredicate(_) => VALIDATION_CODE_UNSUPPORTED_PREDICATE,
        _ => VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
    }
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

fn parse_object_parts(raw: &str) -> Vec<String> {
    raw.split('.')
        .map(normalize_identifier)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
}

fn canonicalize_table_namespace(
    raw_table_name: &str,
    default_database: &str,
    default_schema: &str,
) -> (String, String, String) {
    let parts = parse_object_parts(raw_table_name);
    match parts.as_slice() {
        [table] => (
            normalize_identifier(default_database),
            normalize_identifier(default_schema),
            table.clone(),
        ),
        [schema, table] => (
            normalize_identifier(default_database),
            schema.clone(),
            table.clone(),
        ),
        [database, schema, table] => (database.clone(), schema.clone(), table.clone()),
        _ => (
            normalize_identifier(default_database),
            normalize_identifier(default_schema),
            normalize_identifier(raw_table_name),
        ),
    }
}

fn split_once_case_insensitive<'a>(haystack: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let lower_haystack = haystack.to_ascii_lowercase();
    let lower_needle = needle.to_ascii_lowercase();
    lower_haystack.find(&lower_needle).map(|index| {
        let head = &haystack[..index];
        let tail = &haystack[index + needle.len()..];
        (head, tail)
    })
}

fn parse_order_by_from_sql(sql: &str) -> Vec<SortSpec> {
    let (_, after_order_by) = match split_once_case_insensitive(sql, "order by") {
        Some(parts) => parts,
        None => return Vec::new(),
    };

    let mut order_clause = after_order_by.trim();
    for stopper in [" limit ", " offset ", " fetch "] {
        if let Some((head, _)) = split_once_case_insensitive(order_clause, stopper) {
            order_clause = head.trim();
        }
    }

    if order_clause.is_empty() {
        return Vec::new();
    }

    order_clause
        .split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let lower = segment.to_ascii_lowercase();
            if lower.ends_with(" desc") {
                SortSpec {
                    expression: segment[..segment.len() - 5].trim().to_string(),
                    ascending: false,
                }
            } else if lower.ends_with(" asc") {
                SortSpec {
                    expression: segment[..segment.len() - 4].trim().to_string(),
                    ascending: true,
                }
            } else {
                SortSpec {
                    expression: segment.to_string(),
                    ascending: true,
                }
            }
        })
        .filter(|spec| !spec.expression.is_empty())
        .collect::<Vec<_>>()
}

/// What: Parse a SQL numeric clause token as non-negative integer.
///
/// Inputs:
/// - `raw`: Clause token text.
/// - `label`: Clause label used in validation errors.
///
/// Output:
/// - Parsed unsigned integer value.
fn parse_non_negative_u64_token(raw: &str, label: &str) -> Result<u64, QueryModelError> {
    let token = raw.split_whitespace().next().unwrap_or_default();
    token.parse::<u64>().map_err(|_| {
        QueryModelError::UnsupportedLimitExpression(format!("{}='{}'", label, raw.trim()))
    })
}

/// What: Extract LIMIT and OFFSET values from canonical SQL text.
///
/// Inputs:
/// - `sql`: Canonical SQL text from parsed query AST.
///
/// Output:
/// - Tuple with optional `(limit, offset)` values.
fn parse_limit_offset_from_sql(sql: &str) -> Result<(Option<u64>, Option<u64>), QueryModelError> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains(" fetch ") {
        return Err(QueryModelError::UnsupportedFetchClause);
    }

    let limit = if let Some((_, after_limit)) = split_once_case_insensitive(sql, " limit ") {
        let limit_raw =
            if let Some((head, _)) = split_once_case_insensitive(after_limit, " offset ") {
                head.trim()
            } else {
                after_limit.trim()
            };

        if limit_raw.is_empty() {
            return Err(QueryModelError::UnsupportedLimitExpression(
                "LIMIT clause is empty".to_string(),
            ));
        }

        Some(parse_non_negative_u64_token(limit_raw, "LIMIT")?)
    } else {
        None
    };

    let offset = if let Some((_, after_offset)) = split_once_case_insensitive(sql, " offset ") {
        let offset_raw = after_offset.trim();
        if offset_raw.is_empty() {
            return Err(QueryModelError::UnsupportedLimitExpression(
                "OFFSET clause is empty".to_string(),
            ));
        }
        Some(parse_non_negative_u64_token(offset_raw, "OFFSET")?)
    } else {
        None
    };

    if offset.is_some() && limit.is_none() {
        return Err(QueryModelError::OffsetWithoutLimit);
    }

    Ok((limit, offset))
}

fn extract_minimal_select(query: &SqlQuery) -> Result<&Select, QueryModelError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err(QueryModelError::UnsupportedSetOperation),
    };

    if select.from.len() != 1 {
        return Err(QueryModelError::UnsupportedMultiTableFrom);
    }

    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(QueryModelError::UnsupportedJoin);
    }

    Ok(select)
}

/// What: Build a shared dispatch envelope for a minimal SELECT query.
///
/// Inputs:
/// - `query`: Parsed SQL query AST.
/// - `session_id`: Current session identifier.
/// - `default_database`: Session default database fallback.
/// - `default_schema`: Session default schema fallback.
///
/// Output:
/// - Canonical dispatch envelope containing serialized payload and namespace fields.
///
/// Details:
/// - This function centralizes query payload construction outside the server crate so
///   planner and other crates can share the same model boundary.
pub fn build_select_query_dispatch_envelope(
    query: &SqlQuery,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<QueryDispatchEnvelope, QueryModelError> {
    let select = extract_minimal_select(query)?;

    let from = &select.from[0];
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(QueryModelError::UnsupportedTableFactor),
    };

    let (database, schema, table) =
        canonicalize_table_namespace(&table_name, default_database, default_schema);

    let canonical_sql = query.to_string();
    let (limit, offset) = parse_limit_offset_from_sql(canonical_sql.as_str())?;
    let model = SelectQueryModel {
        version: QUERY_PAYLOAD_VERSION,
        statement: "Select".to_string(),
        session_id: session_id.to_string(),
        namespace: QueryNamespace {
            database: database.clone(),
            schema: schema.clone(),
            table: table.clone(),
            raw: table_name,
        },
        projection: select
            .projection
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>(),
        selection: select
            .selection
            .as_ref()
            .map(std::string::ToString::to_string),
        order_by: parse_order_by_from_sql(canonical_sql.as_str()),
        limit,
        offset,
        sql: canonical_sql,
    };

    let logical_plan = crate::planner::build_logical_plan_from_select_model(&model)
        .map_err(|e| QueryModelError::PlannerTranslationFailed(e.to_string()))?;
    let physical_plan = crate::planner::build_physical_plan_from_logical_plan(&logical_plan)
        .map_err(|e| match e {
            crate::planner::PlannerError::InvalidPhysicalPipeline(message) => {
                QueryModelError::InvalidPhysicalPipeline(message)
            }
            crate::planner::PlannerError::UnsupportedPhysicalOperator(name) => {
                QueryModelError::UnsupportedPhysicalOperator(name)
            }
            crate::planner::PlannerError::UnsupportedPredicate(name) => {
                QueryModelError::UnsupportedPredicate(name)
            }
            _ => QueryModelError::PlannerPhysicalFailed(e.to_string()),
        })?;
    let logical_plan_text = crate::planner::explain::explain_logical_plan(&logical_plan);
    let physical_plan_text = crate::planner::explain::explain_physical_plan(&physical_plan);
    let physical_pipeline = physical_plan
        .operators
        .iter()
        .map(|op| op.canonical_name().to_string())
        .collect::<Vec<_>>();

    let payload = json!({
        "version": model.version,
        "statement": model.statement,
        "session_id": model.session_id,
        "namespace": model.namespace,
        "projection": model.projection,
        "selection": model.selection,
        "order_by": model.order_by,
        "limit": model.limit,
        "offset": model.offset,
        "sql": model.sql,
        "logical_plan": logical_plan,
        "physical_plan": physical_plan,
        "diagnostics": {
            "logical_plan_text": logical_plan_text,
            "physical_plan_text": physical_plan_text,
            "physical_pipeline": physical_pipeline,
        },
    })
    .to_string();

    Ok(QueryDispatchEnvelope {
        payload,
        database,
        schema,
        table,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR,
        VALIDATION_CODE_UNSUPPORTED_PIPELINE, VALIDATION_CODE_UNSUPPORTED_PREDICATE,
        VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE, build_select_query_dispatch_envelope,
        validation_code_for_query_error,
    };
    use crate::parser::sql::parse_query;
    use serde_json::Value;

    #[test]
    fn builds_payload_for_minimal_select() {
        let statements = parse_query("SELECT id FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect("payload should build");

        assert!(envelope.payload.contains("\"statement\":\"Select\""));
        assert!(envelope.payload.contains("\"database\":\"sales\""));
        assert!(envelope.payload.contains("\"logical_plan\""));
        assert!(envelope.payload.contains("\"physical_plan\""));
        assert!(envelope.payload.contains("\"diagnostics\""));
        assert_eq!(envelope.table, "users");
    }

    #[test]
    fn captures_order_by_in_payload() {
        let statements =
            parse_query("SELECT id FROM sales.public.users ORDER BY id DESC LIMIT 5 OFFSET 2")
                .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect("payload should build");
        let parsed: Value =
            serde_json::from_str(&envelope.payload).expect("payload should be valid json");
        let order_by = parsed
            .get("order_by")
            .and_then(Value::as_array)
            .expect("payload should include order_by");

        assert_eq!(order_by.len(), 1);
        assert_eq!(
            order_by[0].get("expression").and_then(Value::as_str),
            Some("id")
        );
        assert_eq!(
            order_by[0].get("ascending").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(parsed.get("limit").and_then(Value::as_u64), Some(5));
        assert_eq!(parsed.get("offset").and_then(Value::as_u64), Some(2));

        let diagnostics = parsed
            .get("diagnostics")
            .expect("payload should include diagnostics");
        let physical_text = diagnostics
            .get("physical_plan_text")
            .and_then(Value::as_str)
            .expect("diagnostics should include physical_plan_text");
        assert!(physical_text.contains("Sort(id DESC)"));
    }

    #[test]
    fn rejects_offset_without_limit() {
        let statements = parse_query("SELECT id FROM sales.public.users OFFSET 2")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect_err("must reject offset without limit");
        assert!(matches!(err, QueryModelError::OffsetWithoutLimit));
    }

    #[test]
    fn rejects_fetch_clause() {
        let statements = parse_query("SELECT id FROM sales.public.users FETCH FIRST 1 ROWS ONLY")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect_err("must reject fetch");
        assert!(matches!(err, QueryModelError::UnsupportedFetchClause));
    }

    #[test]
    fn rejects_multi_table_select() {
        let statements = parse_query("SELECT * FROM a, b").expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "default", "public")
            .expect_err("must reject unsupported shape");

        assert!(err.to_string().contains("exactly one table"));
    }

    #[test]
    fn payload_contains_logical_plan_structure() {
        let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect("payload should build");

        let parsed: Value =
            serde_json::from_str(&envelope.payload).expect("payload should be valid json");

        assert_eq!(
            parsed.get("statement").and_then(Value::as_str),
            Some("Select")
        );
        assert_eq!(parsed.get("version").and_then(Value::as_u64), Some(2));

        let namespace = parsed
            .get("namespace")
            .expect("payload should include namespace");
        assert_eq!(
            namespace.get("database").and_then(Value::as_str),
            Some("sales")
        );
        assert_eq!(
            namespace.get("schema").and_then(Value::as_str),
            Some("public")
        );
        assert_eq!(
            namespace.get("table").and_then(Value::as_str),
            Some("users")
        );

        let logical_plan = parsed
            .get("logical_plan")
            .expect("payload should include logical_plan");
        let relation = logical_plan
            .get("relation")
            .expect("logical_plan should include relation");
        assert_eq!(
            relation.get("database").and_then(Value::as_str),
            Some("sales")
        );
        assert_eq!(
            relation.get("schema").and_then(Value::as_str),
            Some("public")
        );
        assert_eq!(relation.get("table").and_then(Value::as_str), Some("users"));

        let projection_exprs = logical_plan
            .get("projection")
            .and_then(|p| p.get("expressions"))
            .and_then(Value::as_array)
            .expect("logical_plan.projection.expressions should be an array");
        assert_eq!(projection_exprs.len(), 2);

        let physical_plan = parsed
            .get("physical_plan")
            .expect("payload should include physical_plan");
        let operators = physical_plan
            .get("operators")
            .and_then(Value::as_array)
            .expect("physical_plan.operators should be an array");
        assert!(operators.len() >= 3);
    }

    #[test]
    fn rejects_deferred_predicate_shapes() {
        let statements = parse_query("SELECT id FROM sales.public.users WHERE name LIKE 'a%'")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect_err("must reject deferred predicate");

        match err {
            QueryModelError::UnsupportedPredicate(name) => assert_eq!(name, "LIKE"),
            other => panic!("unexpected error: {}", other),
        }
    }

    #[test]
    fn maps_validation_codes_from_query_model_errors() {
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::InvalidPhysicalPipeline(
                "pipeline must end with materialize".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_PIPELINE
        );
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::UnsupportedPhysicalOperator(
                "HashJoin".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_OPERATOR
        );
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::UnsupportedPredicate(
                "LIKE".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_PREDICATE
        );
    }

    #[test]
    fn maps_all_query_model_error_variants_to_expected_codes() {
        let cases = vec![
            (
                QueryModelError::UnsupportedSetOperation,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::UnsupportedMultiTableFrom,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::UnsupportedJoin,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::UnsupportedTableFactor,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::PlannerTranslationFailed("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::PlannerPhysicalFailed("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::InvalidPhysicalPipeline("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_PIPELINE,
            ),
            (
                QueryModelError::UnsupportedPhysicalOperator("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_OPERATOR,
            ),
            (
                QueryModelError::UnsupportedPredicate("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_PREDICATE,
            ),
            (
                QueryModelError::UnsupportedLimitExpression("x".to_string()),
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::UnsupportedFetchClause,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
            (
                QueryModelError::OffsetWithoutLimit,
                VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
            ),
        ];

        for (err, expected_code) in cases {
            assert_eq!(
                validation_code_for_query_error(&err),
                expected_code,
                "unexpected validation code for error variant: {err:?}"
            );
        }
    }
}
