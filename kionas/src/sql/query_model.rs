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

/// What: Shared semantic model for a minimal SELECT query.
///
/// Inputs:
/// - `version`: Canonical payload version.
/// - `statement`: Query statement kind.
/// - `session_id`: Session identifier.
/// - `namespace`: Resolved query namespace metadata.
/// - `projection`: Projection expressions.
/// - `selection`: Optional filter expression.
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
        }
    }
}

impl std::error::Error for QueryModelError {}

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
        sql: query.to_string(),
    };

    let logical_plan = crate::planner::build_logical_plan_from_select_model(&model)
        .map_err(|e| QueryModelError::PlannerTranslationFailed(e.to_string()))?;

    let payload = json!({
        "version": model.version,
        "statement": model.statement,
        "session_id": model.session_id,
        "namespace": model.namespace,
        "projection": model.projection,
        "selection": model.selection,
        "sql": model.sql,
        "logical_plan": logical_plan,
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
    use super::build_select_query_dispatch_envelope;
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
        assert_eq!(envelope.table, "users");
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
    }
}
