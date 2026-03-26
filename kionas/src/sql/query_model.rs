use crate::parser::datafusion_sql::sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Query as SqlQuery, Select, SetExpr,
    SetOperator, SetQuantifier, TableFactor,
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
pub const VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE: &str = "VALIDATION_UNSUPPORTED_QUERY_SHAPE";
/// What: Validation outcome code for invalid physical pipeline errors.
pub const VALIDATION_CODE_UNSUPPORTED_PIPELINE: &str = "VALIDATION_UNSUPPORTED_PIPELINE";
/// What: Validation outcome code for unsupported physical operators.
pub const VALIDATION_CODE_UNSUPPORTED_OPERATOR: &str = "VALIDATION_UNSUPPORTED_OPERATOR";
/// What: Validation outcome code for unsupported predicates.
pub const VALIDATION_CODE_UNSUPPORTED_PREDICATE: &str = "VALIDATION_UNSUPPORTED_PREDICATE";

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

/// What: Supported join type in query-model payload.
///
/// Inputs:
/// - Variant selected during AST extraction.
///
/// Output:
/// - Stable join type consumed by logical translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueryJoinType {
    Inner,
}

/// What: One equi-join key pair from a JOIN ON clause.
///
/// Inputs:
/// - `left`: Left expression text.
/// - `right`: Right expression text.
///
/// Output:
/// - Serializable join key pair entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryJoinKey {
    pub left: String,
    pub right: String,
}

/// What: Canonical query-model representation for one JOIN clause.
///
/// Inputs:
/// - `join_type`: Supported join variant.
/// - `right`: Canonical right relation namespace.
/// - `keys`: Equi-join key pairs.
///
/// Output:
/// - Serializable join directive consumed by logical planner translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryJoinSpec {
    pub join_type: QueryJoinType,
    pub right: QueryNamespace,
    pub keys: Vec<QueryJoinKey>,
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
/// - `group_by`: Optional GROUP BY directives.
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
    pub joins: Vec<QueryJoinSpec>,
    pub group_by: Vec<String>,
    pub order_by: Vec<SortSpec>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    #[serde(default)]
    pub union: Option<QueryUnionSpec>,
    pub sql: String,
}

/// What: UNION query semantics captured in the canonical query model.
///
/// Inputs:
/// - `operands`: Flattened operand SELECT models in SQL evaluation order.
/// - `distinct`: `true` for UNION DISTINCT semantics, `false` for UNION ALL.
///
/// Output:
/// - Serializable UNION metadata consumed by planner and worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryUnionSpec {
    pub operands: Vec<SelectQueryModel>,
    pub distinct: bool,
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

/// What: Canonical SELECT dispatch model bundle without planning artifacts.
///
/// Inputs:
/// - `model`: Canonical semantic model extracted from SQL AST.
/// - `database`: Canonical database identifier.
/// - `schema`: Canonical schema identifier.
/// - `table`: Canonical table identifier.
///
/// Output:
/// - Reusable bundle consumed by server-owned planning and payload emission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectQueryDispatchModel {
    pub model: SelectQueryModel,
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
    UnionOperandSchemaMismatch(String),
    UnsupportedMultiTableFrom,
    UnsupportedJoin,
    UnsupportedJoinType(String),
    UnsupportedJoinConstraint,
    UnsupportedJoinExpression(String),
    MissingJoinKeys,
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
            QueryModelError::UnionOperandSchemaMismatch(message) => {
                write!(f, "UNION operand schema mismatch: {}", message)
            }
            QueryModelError::UnsupportedMultiTableFrom => {
                write!(f, "query must reference exactly one table in FROM")
            }
            QueryModelError::UnsupportedJoin => {
                write!(f, "JOIN is not supported in this phase")
            }
            QueryModelError::UnsupportedJoinType(kind) => {
                write!(f, "JOIN type '{}' is not supported in this phase", kind)
            }
            QueryModelError::UnsupportedJoinConstraint => {
                write!(f, "JOIN must use an ON clause in this phase")
            }
            QueryModelError::UnsupportedJoinExpression(message) => {
                write!(f, "unsupported JOIN expression: {}", message)
            }
            QueryModelError::MissingJoinKeys => {
                write!(f, "JOIN must include at least one equality key")
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

fn parse_group_by_from_sql(sql: &str) -> Vec<String> {
    let (_, after_group_by) = match split_once_case_insensitive(sql, "group by") {
        Some(parts) => parts,
        None => return Vec::new(),
    };

    let mut group_clause = after_group_by.trim();
    for stopper in [" order by ", " limit ", " offset ", " fetch ", " having "] {
        if let Some((head, _)) = split_once_case_insensitive(group_clause, stopper) {
            group_clause = head.trim();
        }
    }

    if group_clause.is_empty() {
        return Vec::new();
    }

    group_clause
        .split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(std::string::ToString::to_string)
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

fn extract_union_select_operands(
    expr: &SetExpr,
    union_distinct_flags: &mut Vec<bool>,
    operands: &mut Vec<Select>,
) -> Result<(), QueryModelError> {
    match expr {
        SetExpr::Select(select) => {
            operands.push(select.as_ref().clone());
            Ok(())
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
            ..
        } if *op == SetOperator::Union => {
            let is_distinct = !matches!(
                set_quantifier,
                SetQuantifier::All | SetQuantifier::AllByName
            );
            union_distinct_flags.push(is_distinct);
            extract_union_select_operands(left.as_ref(), union_distinct_flags, operands)?;
            extract_union_select_operands(right.as_ref(), union_distinct_flags, operands)
        }
        _ => Err(QueryModelError::UnsupportedSetOperation),
    }
}

fn build_select_query_dispatch_model_from_select(
    select: &Select,
    canonical_sql: String,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<SelectQueryDispatchModel, QueryModelError> {
    if select.from.len() != 1 {
        return Err(QueryModelError::UnsupportedMultiTableFrom);
    }

    let from = &select.from[0];
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(QueryModelError::UnsupportedTableFactor),
    };

    let (database, schema, table) =
        canonicalize_table_namespace(&table_name, default_database, default_schema);

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
        joins: parse_join_specs(&from.joins, default_database, default_schema)?,
        group_by: parse_group_by_from_sql(canonical_sql.as_str()),
        order_by: parse_order_by_from_sql(canonical_sql.as_str()),
        limit,
        offset,
        union: None,
        sql: canonical_sql,
    };

    Ok(SelectQueryDispatchModel {
        model,
        database,
        schema,
        table,
    })
}

fn parse_join_column_ref(expr: &Expr) -> Result<String, QueryModelError> {
    match expr {
        Expr::Identifier(identifier) => Ok(identifier.to_string()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(".")),
        _ => Err(QueryModelError::UnsupportedJoinExpression(expr.to_string())),
    }
}

fn parse_join_keys(expr: &Expr, out: &mut Vec<QueryJoinKey>) -> Result<(), QueryModelError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                parse_join_keys(left, out)?;
                parse_join_keys(right, out)?;
                Ok(())
            }
            BinaryOperator::Eq => {
                let left_key = parse_join_column_ref(left)?;
                let right_key = parse_join_column_ref(right)?;
                out.push(QueryJoinKey {
                    left: left_key,
                    right: right_key,
                });
                Ok(())
            }
            _ => Err(QueryModelError::UnsupportedJoinExpression(expr.to_string())),
        },
        _ => Err(QueryModelError::UnsupportedJoinExpression(expr.to_string())),
    }
}

fn parse_join_specs(
    joins: &[Join],
    default_database: &str,
    default_schema: &str,
) -> Result<Vec<QueryJoinSpec>, QueryModelError> {
    let mut specs = Vec::with_capacity(joins.len());

    for join in joins {
        let right_table = match &join.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(QueryModelError::UnsupportedTableFactor),
        };

        let (join_type, constraint) = match &join.join_operator {
            JoinOperator::Inner(constraint) => (QueryJoinType::Inner, constraint),
            JoinOperator::Join(constraint) => (QueryJoinType::Inner, constraint),
            _ => {
                return Err(QueryModelError::UnsupportedJoinType(
                    "non-inner".to_string(),
                ));
            }
        };

        let on = match constraint {
            JoinConstraint::On(expr) => expr,
            _ => return Err(QueryModelError::UnsupportedJoinConstraint),
        };

        let mut keys = Vec::new();
        parse_join_keys(on, &mut keys)?;
        if keys.is_empty() {
            return Err(QueryModelError::MissingJoinKeys);
        }

        let (database, schema, table) =
            canonicalize_table_namespace(&right_table, default_database, default_schema);

        specs.push(QueryJoinSpec {
            join_type,
            right: QueryNamespace {
                database,
                schema,
                table,
                raw: right_table,
            },
            keys,
        });
    }

    Ok(specs)
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
pub async fn build_select_query_dispatch_envelope(
    query: &SqlQuery,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<QueryDispatchEnvelope, QueryModelError> {
    let select_query =
        build_select_query_model(query, session_id, default_database, default_schema)?;
    let database = select_query.database;
    let schema = select_query.schema;
    let table = select_query.table;
    let model = select_query.model;

    let payload = json!({
        "version": model.version,
        "statement": model.statement,
        "session_id": model.session_id,
        "namespace": model.namespace,
        "projection": model.projection,
        "selection": model.selection,
        "joins": model.joins,
        "group_by": model.group_by,
        "order_by": model.order_by,
        "limit": model.limit,
        "offset": model.offset,
        "union": model.union,
        "sql": model.sql,
    })
    .to_string();

    Ok(QueryDispatchEnvelope {
        payload,
        database,
        schema,
        table,
    })
}

/// What: Build canonical SELECT query model and namespace metadata from SQL AST.
///
/// Inputs:
/// - `query`: Parsed SQL query AST.
/// - `session_id`: Current session identifier.
/// - `default_database`: Session default database fallback.
/// - `default_schema`: Session default schema fallback.
///
/// Output:
/// - `SelectQueryDispatchModel` containing canonical semantic model and relation namespace.
pub fn build_select_query_model(
    query: &SqlQuery,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<SelectQueryDispatchModel, QueryModelError> {
    let canonical_sql = query.to_string();
    match query.body.as_ref() {
        SetExpr::Select(select) => build_select_query_dispatch_model_from_select(
            select.as_ref(),
            canonical_sql,
            session_id,
            default_database,
            default_schema,
        ),
        SetExpr::SetOperation {
            op: SetOperator::Union,
            ..
        } => {
            let mut union_distinct_flags = Vec::<bool>::new();
            let mut operand_selects = Vec::<Select>::new();
            extract_union_select_operands(
                query.body.as_ref(),
                &mut union_distinct_flags,
                &mut operand_selects,
            )?;

            if operand_selects.len() < 2 {
                return Err(QueryModelError::UnsupportedSetOperation);
            }

            let mut operand_models = Vec::<SelectQueryModel>::with_capacity(operand_selects.len());
            for operand_select in &operand_selects {
                let operand_dispatch = build_select_query_dispatch_model_from_select(
                    operand_select,
                    operand_select.to_string(),
                    session_id,
                    default_database,
                    default_schema,
                )?;
                operand_models.push(operand_dispatch.model);
            }

            let first_projection = operand_models
                .first()
                .map(|model| model.projection.len())
                .unwrap_or_default();
            for (index, operand) in operand_models.iter().enumerate().skip(1) {
                if operand.projection.len() != first_projection {
                    return Err(QueryModelError::UnionOperandSchemaMismatch(format!(
                        "operand {} has {} projection expressions, expected {}",
                        index,
                        operand.projection.len(),
                        first_projection
                    )));
                }
            }

            let mut root_dispatch = build_select_query_dispatch_model_from_select(
                operand_selects
                    .first()
                    .ok_or(QueryModelError::UnsupportedSetOperation)?,
                canonical_sql.clone(),
                session_id,
                default_database,
                default_schema,
            )?;

            let (root_limit, root_offset) = parse_limit_offset_from_sql(canonical_sql.as_str())?;
            root_dispatch.model.selection = None;
            root_dispatch.model.joins.clear();
            root_dispatch.model.group_by = parse_group_by_from_sql(canonical_sql.as_str());
            root_dispatch.model.order_by = parse_order_by_from_sql(canonical_sql.as_str());
            root_dispatch.model.limit = root_limit;
            root_dispatch.model.offset = root_offset;
            root_dispatch.model.union = Some(QueryUnionSpec {
                operands: operand_models,
                distinct: union_distinct_flags.iter().any(|flag| *flag),
            });
            root_dispatch.model.sql = canonical_sql;

            Ok(root_dispatch)
        }
        _ => Err(QueryModelError::UnsupportedSetOperation),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR,
        VALIDATION_CODE_UNSUPPORTED_PIPELINE, VALIDATION_CODE_UNSUPPORTED_PREDICATE,
        VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE, build_select_query_dispatch_envelope,
        build_select_query_model, validation_code_for_query_error,
    };
    use crate::parser::sql::parse_query;
    use serde_json::Value;

    #[tokio::test]
    async fn builds_payload_for_minimal_select() {
        let statements = parse_query("SELECT id FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
            .expect("payload should build");

        assert!(envelope.payload.contains("\"statement\":\"Select\""));
        assert!(envelope.payload.contains("\"database\":\"sales\""));
        assert_eq!(envelope.table, "users");
    }

    #[tokio::test]
    async fn captures_order_by_in_payload() {
        let statements =
            parse_query("SELECT id FROM sales.public.users ORDER BY id DESC LIMIT 5 OFFSET 2")
                .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
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

        assert!(parsed.get("diagnostics").is_none());
        assert!(parsed.get("physical_plan").is_none());
    }

    #[tokio::test]
    async fn rejects_offset_without_limit() {
        let statements = parse_query("SELECT id FROM sales.public.users OFFSET 2")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
            .expect_err("must reject offset without limit");
        assert!(matches!(err, QueryModelError::OffsetWithoutLimit));
    }

    #[tokio::test]
    async fn rejects_fetch_clause() {
        let statements = parse_query("SELECT id FROM sales.public.users FETCH FIRST 1 ROWS ONLY")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
            .expect_err("must reject fetch");
        assert!(matches!(err, QueryModelError::UnsupportedFetchClause));
    }

    #[tokio::test]
    async fn builds_payload_when_relation_does_not_exist_in_runtime_catalog() {
        let statements = parse_query("SELECT id FROM abc.schema1.table4 ORDER BY id")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "abc", "schema1")
            .await
            .expect("payload should build without runtime catalog lookups");

        let parsed: Value =
            serde_json::from_str(&envelope.payload).expect("payload should be valid json");
        assert_eq!(
            parsed.get("sql").and_then(Value::as_str),
            Some("SELECT id FROM abc.schema1.table4 ORDER BY id")
        );
    }

    #[tokio::test]
    async fn rejects_multi_table_select() {
        let statements = parse_query("SELECT * FROM a, b").expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "default", "public")
            .await
            .expect_err("must reject unsupported shape");

        assert!(err.to_string().contains("exactly one table"));
    }

    #[tokio::test]
    async fn payload_contains_query_model_structure() {
        let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
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

        assert!(parsed.get("logical_plan").is_none());
        assert!(parsed.get("physical_plan").is_none());
    }

    #[tokio::test]
    async fn accepts_deferred_predicate_shapes_in_model_only_envelope() {
        let statements = parse_query("SELECT id FROM sales.public.users WHERE name LIKE 'a%'")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let envelope = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
            .expect("model-only envelope should not reject deferred predicates");
        assert!(envelope.payload.contains("name LIKE 'a%'"));
    }

    #[tokio::test]
    async fn builds_select_query_model_without_planning_artifacts() {
        let statements = parse_query("SELECT id FROM sales.public.users ORDER BY id DESC LIMIT 5")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "sales", "public")
            .expect("query model should build");

        assert_eq!(model.database, "sales");
        assert_eq!(model.schema, "public");
        assert_eq!(model.table, "users");
        assert_eq!(model.model.limit, Some(5));
        assert_eq!(model.model.order_by.len(), 1);
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
                QueryModelError::UnionOperandSchemaMismatch("x".to_string()),
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

    #[test]
    fn builds_union_query_model_with_flattened_operands() {
        let statements = parse_query(
            "SELECT id FROM sales.public.users UNION ALL SELECT id FROM sales.public.users_ca UNION ALL SELECT id FROM sales.public.users_mx",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "sales", "public")
            .expect("union query model should build")
            .model;

        let union = model.union.expect("union spec should be present");
        assert_eq!(union.operands.len(), 3);
        assert!(!union.distinct);
        assert_eq!(union.operands[0].namespace.table, "users");
        assert_eq!(union.operands[1].namespace.table, "users_ca");
        assert_eq!(union.operands[2].namespace.table, "users_mx");
    }

    #[test]
    fn preserves_union_operand_where_clauses() {
        let statements = parse_query(
            "SELECT id, name FROM bench.seed1.customers WHERE id <= 5 UNION ALL SELECT id, name FROM bench.seed1.customers WHERE id > 5 AND id <= 10 ORDER BY id",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "bench", "seed1")
            .expect("union query model should build")
            .model;

        let union = model.union.expect("union spec should be present");
        assert_eq!(union.operands.len(), 2);
        assert_eq!(union.operands[0].selection.as_deref(), Some("id <= 5"));
        assert_eq!(
            union.operands[1].selection.as_deref(),
            Some("id > 5 AND id <= 10")
        );
    }

    #[tokio::test]
    async fn rejects_union_with_projection_mismatch() {
        let statements = parse_query(
            "SELECT id FROM sales.public.users UNION SELECT id, name FROM sales.public.users_ca",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .await
            .expect_err("union operands with mismatched projection size must be rejected");

        assert!(matches!(
            err,
            QueryModelError::UnionOperandSchemaMismatch(_)
        ));
    }
}
