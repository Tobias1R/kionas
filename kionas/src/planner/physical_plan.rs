use crate::planner::logical_plan::{LogicalExpr, LogicalRelation};
use serde::{Deserialize, Serialize};

/// What: Phase 2 physical expression model carried by physical operators.
///
/// Inputs:
/// - Expression-specific fields vary by variant.
///
/// Output:
/// - Typed expression nodes used by physical filter and projection operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalExpr {
    ColumnRef { name: String },
    Raw { sql: String },
}

/// What: One physical ORDER BY expression.
///
/// Inputs:
/// - `expression`: Sort key expression.
/// - `ascending`: `true` for ASC, `false` for DESC.
///
/// Output:
/// - Serializable sort directive consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalSortExpr {
    pub expression: PhysicalExpr,
    pub ascending: bool,
}

/// What: Physical LIMIT/OFFSET directive.
///
/// Inputs:
/// - `count`: Maximum number of rows to return after offset.
/// - `offset`: Number of rows to skip before returning rows.
///
/// Output:
/// - Serializable limit directive consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalLimitSpec {
    pub count: u64,
    pub offset: u64,
}

/// What: Supported and cataloged physical operators for query execution planning.
///
/// Inputs:
/// - Operator-specific fields vary by variant.
///
/// Output:
/// - Serializable physical operator nodes that can be validated before dispatch.
///
/// Details:
/// - `TableScan`, `Filter`, `Projection`, and `Materialize` are implemented in Phase 2.
/// - Remaining operators are cataloged for future phases and must be rejected by capability checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalOperator {
    TableScan { relation: LogicalRelation },
    Filter { predicate: PhysicalExpr },
    Projection { expressions: Vec<PhysicalExpr> },
    Materialize,

    HashJoin { on: Vec<String> },
    NestedLoopJoin,
    AggregatePartial,
    AggregateFinal,
    Sort { keys: Vec<PhysicalSortExpr> },
    Limit { spec: PhysicalLimitSpec },
    ExchangeShuffle { keys: Vec<String> },
    ExchangeBroadcast,
    Repartition,
    Union,
    Values,
}

impl PhysicalOperator {
    /// What: Return the canonical operator name for explain output and diagnostics.
    ///
    /// Inputs:
    /// - `self`: Physical operator variant.
    ///
    /// Output:
    /// - Stable operator name string.
    pub fn canonical_name(&self) -> &'static str {
        match self {
            PhysicalOperator::TableScan { .. } => "TableScan",
            PhysicalOperator::Filter { .. } => "Filter",
            PhysicalOperator::Projection { .. } => "Projection",
            PhysicalOperator::Materialize => "Materialize",
            PhysicalOperator::HashJoin { .. } => "HashJoin",
            PhysicalOperator::NestedLoopJoin => "NestedLoopJoin",
            PhysicalOperator::AggregatePartial => "AggregatePartial",
            PhysicalOperator::AggregateFinal => "AggregateFinal",
            PhysicalOperator::Sort { .. } => "Sort",
            PhysicalOperator::Limit { .. } => "Limit",
            PhysicalOperator::ExchangeShuffle { .. } => "ExchangeShuffle",
            PhysicalOperator::ExchangeBroadcast => "ExchangeBroadcast",
            PhysicalOperator::Repartition => "Repartition",
            PhysicalOperator::Union => "Union",
            PhysicalOperator::Values => "Values",
        }
    }
}

/// What: Phase 2 physical plan root model.
///
/// Inputs:
/// - `operators`: Ordered operator pipeline.
/// - `sql`: Canonical SQL string used for explain and diagnostics.
///
/// Output:
/// - Deterministic physical plan representation for query payload emission.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalPlan {
    pub operators: Vec<PhysicalOperator>,
    pub sql: String,
}

impl From<&LogicalExpr> for PhysicalExpr {
    /// What: Convert a logical expression into the equivalent physical expression node.
    ///
    /// Inputs:
    /// - `expr`: Logical expression from the validated logical plan.
    ///
    /// Output:
    /// - Physical expression with the same semantic content.
    ///
    /// Details:
    /// - This conversion is intentionally lossless for currently supported variants.
    fn from(expr: &LogicalExpr) -> Self {
        match expr {
            LogicalExpr::Column { name } => PhysicalExpr::ColumnRef { name: name.clone() },
            LogicalExpr::Raw { sql } => PhysicalExpr::Raw { sql: sql.clone() },
        }
    }
}
