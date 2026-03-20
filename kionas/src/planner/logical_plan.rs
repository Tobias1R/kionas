use serde::{Deserialize, Serialize};

use crate::planner::join_spec::LogicalJoinSpec;

/// What: Logical expression for Phase 1 minimal SELECT support.
///
/// Inputs:
/// - Expression-specific fields vary by variant.
///
/// Output:
/// - Typed expression nodes used by logical projection and selection clauses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalExpr {
    Column { name: String },
    Raw { sql: String },
}

/// What: Logical relation node representing a single source table.
///
/// Inputs:
/// - `database`: Canonical database identifier.
/// - `schema`: Canonical schema identifier.
/// - `table`: Canonical table identifier.
///
/// Output:
/// - Typed relation metadata for logical plan roots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalRelation {
    pub database: String,
    pub schema: String,
    pub table: String,
}

/// What: Logical projection node for selected expressions.
///
/// Inputs:
/// - `expressions`: Ordered projection expression list.
///
/// Output:
/// - Projection node preserving deterministic expression order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalProjection {
    pub expressions: Vec<LogicalExpr>,
}

/// What: Optional logical selection node for WHERE filters.
///
/// Inputs:
/// - `predicate`: Selection predicate expression.
///
/// Output:
/// - Predicate node used by planner validation and explain output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalSelection {
    pub predicate: LogicalExpr,
}

/// What: One logical ORDER BY expression.
///
/// Inputs:
/// - `expression`: Sort key expression.
/// - `ascending`: `true` for ASC, `false` for DESC.
///
/// Output:
/// - Deterministic ordering directive consumed by physical translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalSortExpr {
    pub expression: LogicalExpr,
    pub ascending: bool,
}

/// What: Phase 1 logical plan root model.
///
/// Inputs:
/// - `relation`: Single source relation.
/// - `projection`: Projection expression node.
/// - `selection`: Optional filter node.
/// - `joins`: Optional join directives.
/// - `order_by`: Optional ordering directives.
/// - `limit`: Optional row-count limit.
/// - `offset`: Optional row offset applied before limit.
/// - `sql`: Canonical SQL string for explain/debug parity.
///
/// Output:
/// - Serializable logical plan payload for dispatch integration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalPlan {
    pub relation: LogicalRelation,
    pub projection: LogicalProjection,
    pub selection: Option<LogicalSelection>,
    pub joins: Vec<LogicalJoinSpec>,
    pub order_by: Vec<LogicalSortExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub sql: String,
}
