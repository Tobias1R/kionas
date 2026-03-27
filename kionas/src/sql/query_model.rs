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

/// What: Canonical FROM source shape for SELECT query-model payloads.
///
/// Inputs:
/// - Variant selected during SQL AST extraction.
///
/// Output:
/// - Stable source descriptor consumed by server planning and diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueryFromSpec {
    Table {
        namespace: QueryNamespace,
    },
    Derived {
        alias: Option<String>,
        query: Box<SelectQueryModel>,
    },
    CteRef {
        name: String,
    },
}

/// What: Canonical CTE payload entry with nested query model.
///
/// Inputs:
/// - `name`: CTE identifier as referenced by the outer query.
/// - `query`: Nested canonical SELECT query model for the CTE body.
///
/// Output:
/// - Serializable CTE model entry attached to top-level SELECT payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryCteSpec {
    pub name: String,
    pub query: Box<SelectQueryModel>,
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
    pub from: QueryFromSpec,
    pub projection: Vec<String>,
    pub selection: Option<String>,
    pub joins: Vec<QueryJoinSpec>,
    pub group_by: Vec<String>,
    pub order_by: Vec<SortSpec>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    #[serde(default)]
    pub ctes: Vec<QueryCteSpec>,
    #[serde(default)]
    pub relation_dependencies: Vec<QueryNamespace>,
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

fn sanitize_sql_for_top_level_clause_search(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;

        if ch == '\'' {
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                let current = bytes[i] as char;
                if current == '\'' {
                    if i + 1 < bytes.len() && (bytes[i + 1] as char) == '\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                out.push(' ');
                i += 1;
            }
            continue;
        }

        if ch == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '-' {
            out.push(' ');
            i += 2;
            while i < bytes.len() {
                let current = bytes[i] as char;
                if current == '\n' {
                    out.push('\n');
                    i += 1;
                    break;
                }
                out.push(' ');
                i += 1;
            }
            continue;
        }

        if ch == '/' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '*' {
            out.push(' ');
            i += 2;
            while i + 1 < bytes.len() {
                if (bytes[i] as char) == '*' && (bytes[i + 1] as char) == '/' {
                    out.push(' ');
                    out.push(' ');
                    i += 2;
                    break;
                }
                out.push(' ');
                i += 1;
            }
            continue;
        }

        out.push(ch.to_ascii_lowercase());
        i += 1;
    }

    out
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn find_top_level_keyword(sql: &str, keyword: &str, search_start: usize) -> Option<usize> {
    if keyword.is_empty() || search_start >= sql.len() || keyword.len() > sql.len() {
        return None;
    }

    let bytes = sql.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut depth = 0i32;
    let mut index = search_start;

    while index + keyword_bytes.len() <= bytes.len() {
        let ch = bytes[index] as char;
        if ch == '(' {
            depth += 1;
            index += 1;
            continue;
        }

        if ch == ')' {
            depth = (depth - 1).max(0);
            index += 1;
            continue;
        }

        if depth == 0 && &bytes[index..index + keyword_bytes.len()] == keyword_bytes {
            let before_ok = if index == 0 {
                true
            } else {
                !is_identifier_char(bytes[index - 1] as char)
            };
            let after_index = index + keyword_bytes.len();
            let after_ok = if after_index >= bytes.len() {
                true
            } else {
                !is_identifier_char(bytes[after_index] as char)
            };

            if before_ok && after_ok {
                return Some(index);
            }
        }

        index += 1;
    }

    None
}

fn split_top_level_csv(clause: &str) -> Vec<String> {
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut out = Vec::<String>::new();

    for (index, ch) in clause.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            ',' if depth == 0 => {
                let segment = clause[start..index].trim();
                if !segment.is_empty() {
                    out.push(segment.to_string());
                }
                start = index + 1;
            }
            _ => {}
        }
    }

    let tail = clause[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }

    out
}

fn extract_top_level_clause(sql: &str, keyword: &str, stoppers: &[&str]) -> Option<String> {
    let sanitized = sanitize_sql_for_top_level_clause_search(sql);
    let start_index = find_top_level_keyword(&sanitized, keyword, 0)?;
    let clause_start = start_index + keyword.len();

    let mut clause_end = sanitized.len();
    for stopper in stoppers {
        if let Some(index) = find_top_level_keyword(&sanitized, stopper, clause_start)
            && index < clause_end
        {
            clause_end = index;
        }
    }

    let raw = sql[clause_start..clause_end].trim();
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn parse_order_by_from_sql(sql: &str) -> Vec<SortSpec> {
    let Some(order_clause) =
        extract_top_level_clause(sql, "order by", &["limit", "offset", "fetch"])
    else {
        return Vec::new();
    };

    split_top_level_csv(order_clause.as_str())
        .into_iter()
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
    let Some(group_clause) = extract_top_level_clause(
        sql,
        "group by",
        &["having", "order by", "limit", "offset", "fetch"],
    ) else {
        return Vec::new();
    };

    split_top_level_csv(group_clause.as_str())
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

fn merge_relation_dependencies(
    current: &mut Vec<QueryNamespace>,
    incoming: impl IntoIterator<Item = QueryNamespace>,
) {
    for dependency in incoming {
        let exists = current.iter().any(|item| {
            item.database == dependency.database
                && item.schema == dependency.schema
                && item.table == dependency.table
        });
        if !exists {
            current.push(dependency);
        }
    }
}

fn collect_subquery_dependencies_from_expr(
    expr: &Expr,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<Vec<QueryNamespace>, QueryModelError> {
    let mut dependencies = Vec::<QueryNamespace>::new();

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    left,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    right,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    expr,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    expr,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    low,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    high,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
        }
        Expr::InList { expr, list, .. } => {
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    expr,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );

            for item in list {
                merge_relation_dependencies(
                    &mut dependencies,
                    collect_subquery_dependencies_from_expr(
                        item,
                        session_id,
                        default_database,
                        default_schema,
                    )?,
                );
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            merge_relation_dependencies(
                &mut dependencies,
                collect_subquery_dependencies_from_expr(
                    expr,
                    session_id,
                    default_database,
                    default_schema,
                )?,
            );
            let sub_model = build_select_query_model_from_query(
                subquery,
                session_id,
                default_database,
                default_schema,
            )?;
            merge_relation_dependencies(&mut dependencies, sub_model.relation_dependencies);
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            let sub_model = build_select_query_model_from_query(
                subquery,
                session_id,
                default_database,
                default_schema,
            )?;
            merge_relation_dependencies(&mut dependencies, sub_model.relation_dependencies);
        }
        _ => {}
    }

    Ok(dependencies)
}

fn build_from_spec_and_dependencies(
    factor: &TableFactor,
    cte_names: &std::collections::HashSet<String>,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<(QueryFromSpec, Vec<QueryNamespace>), QueryModelError> {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = name.to_string();
            let parts = parse_object_parts(table_name.as_str());
            if parts.len() == 1 {
                let candidate = parts[0].clone();
                if cte_names.contains(candidate.as_str()) {
                    return Ok((
                        QueryFromSpec::CteRef { name: candidate },
                        Vec::<QueryNamespace>::new(),
                    ));
                }
            }

            let (database, schema, table) =
                canonicalize_table_namespace(&table_name, default_database, default_schema);
            let namespace = QueryNamespace {
                database,
                schema,
                table,
                raw: table_name,
            };
            Ok((
                QueryFromSpec::Table {
                    namespace: namespace.clone(),
                },
                vec![namespace],
            ))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let sub_model = build_select_query_model_from_query(
                subquery,
                session_id,
                default_database,
                default_schema,
            )?;
            Ok((
                QueryFromSpec::Derived {
                    alias: alias
                        .as_ref()
                        .map(|table_alias| normalize_identifier(table_alias.name.value.as_str())),
                    query: Box::new(sub_model.clone()),
                },
                sub_model.relation_dependencies,
            ))
        }
        _ => Err(QueryModelError::UnsupportedTableFactor),
    }
}

fn build_select_query_model_from_select(
    select: &Select,
    canonical_sql: String,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
    ctes: Vec<QueryCteSpec>,
    cte_names: &std::collections::HashSet<String>,
) -> Result<SelectQueryModel, QueryModelError> {
    if select.from.len() != 1 {
        return Err(QueryModelError::UnsupportedMultiTableFrom);
    }

    let from = &select.from[0];
    let (from_spec, mut relation_dependencies) = build_from_spec_and_dependencies(
        &from.relation,
        cte_names,
        session_id,
        default_database,
        default_schema,
    )?;

    for join in &from.joins {
        if let TableFactor::Table { name, .. } = &join.relation {
            let table_name = name.to_string();
            let (database, schema, table) =
                canonicalize_table_namespace(&table_name, default_database, default_schema);
            merge_relation_dependencies(
                &mut relation_dependencies,
                [QueryNamespace {
                    database,
                    schema,
                    table,
                    raw: table_name,
                }],
            );
        }
    }

    if let Some(selection) = select.selection.as_ref() {
        let subquery_dependencies = collect_subquery_dependencies_from_expr(
            selection,
            session_id,
            default_database,
            default_schema,
        )?;
        merge_relation_dependencies(&mut relation_dependencies, subquery_dependencies);
    }

    for cte in &ctes {
        merge_relation_dependencies(
            &mut relation_dependencies,
            cte.query.relation_dependencies.iter().cloned(),
        );
    }

    let (limit, offset) = parse_limit_offset_from_sql(canonical_sql.as_str())?;
    Ok(SelectQueryModel {
        version: QUERY_PAYLOAD_VERSION,
        statement: "Select".to_string(),
        session_id: session_id.to_string(),
        from: from_spec,
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
        ctes,
        relation_dependencies,
        union: None,
        sql: canonical_sql,
    })
}

fn build_select_query_model_from_query(
    query: &SqlQuery,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<SelectQueryModel, QueryModelError> {
    let cte_names = query
        .with
        .as_ref()
        .map(|with_clause| {
            with_clause
                .cte_tables
                .iter()
                .map(|cte| normalize_identifier(cte.alias.name.value.as_str()))
                .collect::<std::collections::HashSet<_>>()
        })
        .unwrap_or_default();

    let mut ctes = Vec::<QueryCteSpec>::new();
    if let Some(with_clause) = query.with.as_ref() {
        for cte in &with_clause.cte_tables {
            let cte_model = build_select_query_model_from_query(
                cte.query.as_ref(),
                session_id,
                default_database,
                default_schema,
            )?;
            ctes.push(QueryCteSpec {
                name: normalize_identifier(cte.alias.name.value.as_str()),
                query: Box::new(cte_model),
            });
        }
    }

    match query.body.as_ref() {
        SetExpr::Select(select) => build_select_query_model_from_select(
            select.as_ref(),
            query.to_string(),
            session_id,
            default_database,
            default_schema,
            ctes,
            &cte_names,
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
                let operand_model = build_select_query_model_from_select(
                    operand_select,
                    operand_select.to_string(),
                    session_id,
                    default_database,
                    default_schema,
                    Vec::new(),
                    &std::collections::HashSet::new(),
                )?;
                operand_models.push(operand_model);
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

            let mut root_model = build_select_query_model_from_select(
                operand_selects
                    .first()
                    .ok_or(QueryModelError::UnsupportedSetOperation)?,
                query.to_string(),
                session_id,
                default_database,
                default_schema,
                ctes,
                &cte_names,
            )?;

            let (root_limit, root_offset) = parse_limit_offset_from_sql(root_model.sql.as_str())?;
            root_model.selection = None;
            root_model.joins.clear();
            root_model.group_by = parse_group_by_from_sql(root_model.sql.as_str());
            root_model.order_by = parse_order_by_from_sql(root_model.sql.as_str());
            root_model.limit = root_limit;
            root_model.offset = root_offset;
            root_model.union = Some(QueryUnionSpec {
                operands: operand_models,
                distinct: union_distinct_flags.iter().any(|flag| *flag),
            });

            for cte in &root_model.ctes {
                merge_relation_dependencies(
                    &mut root_model.relation_dependencies,
                    cte.query.relation_dependencies.iter().cloned(),
                );
            }

            Ok(root_model)
        }
        _ => Err(QueryModelError::UnsupportedSetOperation),
    }
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
        "from": model.from,
        "projection": model.projection,
        "selection": model.selection,
        "joins": model.joins,
        "group_by": model.group_by,
        "order_by": model.order_by,
        "limit": model.limit,
        "offset": model.offset,
        "ctes": model.ctes,
        "relation_dependencies": model.relation_dependencies,
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
    let model =
        build_select_query_model_from_query(query, session_id, default_database, default_schema)?;

    let primary = model
        .relation_dependencies
        .first()
        .cloned()
        .ok_or(QueryModelError::UnsupportedTableFactor)?;

    Ok(SelectQueryDispatchModel {
        model,
        database: primary.database,
        schema: primary.schema,
        table: primary.table,
    })
}

/// What: Resolve the primary physical relation dependency used by legacy planner surfaces.
///
/// Inputs:
/// - `model`: Canonical SELECT query model.
///
/// Output:
/// - First canonical physical table dependency when present.
pub fn primary_relation_dependency(model: &SelectQueryModel) -> Option<&QueryNamespace> {
    model.relation_dependencies.first()
}

#[cfg(test)]
mod tests {
    use super::{
        QueryFromSpec, QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR,
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

    #[test]
    fn ignores_window_order_by_inside_cte() {
        let statements = parse_query(
            "WITH ranked_orders AS (SELECT o.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY quantity DESC) AS rn FROM bench4.seed1.orders o) SELECT * FROM ranked_orders WHERE rn <= 3",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "bench4", "seed1")
            .expect("query model should build")
            .model;

        assert!(model.order_by.is_empty());
    }

    #[test]
    fn ignores_group_by_inside_cte_chain_for_outer_select() {
        let statements = parse_query(
            "WITH product_totals AS (SELECT product_id, SUM(quantity) AS total_quantity FROM bench4.seed1.orders GROUP BY product_id), ranked_products AS (SELECT pt.*, RANK() OVER (ORDER BY total_quantity DESC) AS rnk FROM product_totals pt) SELECT * FROM ranked_products WHERE rnk <= 5",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "bench4", "seed1")
            .expect("query model should build")
            .model;

        assert!(model.group_by.is_empty());
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
        assert_eq!(union.operands[0].relation_dependencies[0].table, "users");
        assert_eq!(union.operands[1].relation_dependencies[0].table, "users_ca");
        assert_eq!(union.operands[2].relation_dependencies[0].table, "users_mx");
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

    #[test]
    fn builds_model_for_derived_table_source() {
        let statements =
            parse_query("SELECT id FROM (SELECT id, name FROM sales.public.users) t WHERE id > 10")
                .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "sales", "public")
            .expect("query model should build")
            .model;

        assert!(matches!(model.from, QueryFromSpec::Derived { .. }));
        assert!(
            model
                .relation_dependencies
                .iter()
                .any(|dep| dep.table == "users")
        );
    }

    #[test]
    fn builds_model_for_cte_source_and_dependencies() {
        let statements = parse_query(
            "WITH order_counts AS (SELECT customer_id FROM sales.public.orders) SELECT customer_id FROM order_counts",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "sales", "public")
            .expect("query model should build")
            .model;

        assert_eq!(model.ctes.len(), 1);
        assert!(matches!(model.from, QueryFromSpec::CteRef { .. }));
        assert!(
            model
                .relation_dependencies
                .iter()
                .any(|dep| dep.table == "orders")
        );
    }

    #[test]
    fn collects_in_subquery_dependencies() {
        let statements = parse_query(
            "SELECT id FROM sales.public.users WHERE id IN (SELECT user_id FROM sales.public.orders)",
        )
        .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let model = build_select_query_model(query, "s1", "sales", "public")
            .expect("query model should build")
            .model;

        assert!(
            model
                .relation_dependencies
                .iter()
                .any(|dep| dep.table == "users")
        );
        assert!(
            model
                .relation_dependencies
                .iter()
                .any(|dep| dep.table == "orders")
        );
    }
}
