use crate::planner::aggregate_spec::PhysicalAggregateSpec;
use crate::planner::join_spec::PhysicalJoinSpec;
use crate::planner::logical_plan::{LogicalExpr, LogicalRelation};
use crate::planner::predicate_expr::PredicateExpr;
use crate::sql::datatypes::ColumnDatatypeSpec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    Predicate { predicate: PredicateExpr },
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

/// What: One concrete bound in a SQL window frame.
///
/// Inputs:
/// - Variant payload carries optional offset for relative bounds.
///
/// Output:
/// - Serializable frame bound consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalWindowFrameBound {
    UnboundedPreceding,
    Preceding { offset: u64 },
    CurrentRow,
    Following { offset: u64 },
    UnboundedFollowing,
}

/// What: Supported SQL window frame unit.
///
/// Inputs:
/// - Variant selects frame interpretation strategy.
///
/// Output:
/// - Serializable frame unit consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalWindowFrameUnit {
    Rows,
    Range,
}

/// What: Window frame definition attached to one window function expression.
///
/// Inputs:
/// - `unit`: Frame unit (`ROWS` or `RANGE`).
/// - `start_bound`: Inclusive frame start bound.
/// - `end_bound`: Inclusive frame end bound.
///
/// Output:
/// - Serializable frame contract used during window execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalWindowFrameSpec {
    pub unit: PhysicalWindowFrameUnit,
    pub start_bound: PhysicalWindowFrameBound,
    pub end_bound: PhysicalWindowFrameBound,
}

/// What: One window function expression in a physical operator.
///
/// Inputs:
/// - `function_name`: Canonical function name (for example `ROW_NUMBER`, `SUM`).
/// - `args`: Ordered function arguments.
/// - `output_name`: Projected output column name.
/// - `frame`: Optional frame definition for frame-aware functions.
///
/// Output:
/// - Serializable function descriptor consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalWindowFunctionSpec {
    pub function_name: String,
    #[serde(default)]
    pub args: Vec<PhysicalExpr>,
    pub output_name: String,
    #[serde(default)]
    pub frame: Option<PhysicalWindowFrameSpec>,
}

/// What: Physical window aggregation operator contract.
///
/// Inputs:
/// - `partition_by`: Expressions defining partition boundaries.
/// - `order_by`: Expressions defining order inside partitions.
/// - `functions`: Ordered window function descriptors.
///
/// Output:
/// - Serializable window operator contract consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalWindowSpec {
    #[serde(default)]
    pub partition_by: Vec<PhysicalExpr>,
    #[serde(default)]
    pub order_by: Vec<PhysicalSortExpr>,
    pub functions: Vec<PhysicalWindowFunctionSpec>,
}

/// What: One UNION operand relation in the physical execution contract.
///
/// Inputs:
/// - `relation`: Canonical relation namespace for one UNION input.
///
/// Output:
/// - Serializable UNION operand descriptor consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalUnionOperand {
    pub relation: LogicalRelation,
    #[serde(default)]
    pub filter: Option<PredicateExpr>,
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
    TableScan {
        relation: LogicalRelation,
    },
    Filter {
        predicate: PhysicalExpr,
    },
    Projection {
        expressions: Vec<PhysicalExpr>,
    },
    Materialize,

    HashJoin {
        spec: PhysicalJoinSpec,
    },
    NestedLoopJoin,
    AggregatePartial {
        spec: PhysicalAggregateSpec,
    },
    AggregateFinal {
        spec: PhysicalAggregateSpec,
    },
    WindowAggr {
        spec: PhysicalWindowSpec,
    },
    Sort {
        keys: Vec<PhysicalSortExpr>,
    },
    Limit {
        spec: PhysicalLimitSpec,
    },
    ExchangeShuffle {
        keys: Vec<String>,
    },
    ExchangeBroadcast,
    Repartition,
    Union {
        operands: Vec<PhysicalUnionOperand>,
        distinct: bool,
    },
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
            PhysicalOperator::AggregatePartial { .. } => "AggregatePartial",
            PhysicalOperator::AggregateFinal { .. } => "AggregateFinal",
            PhysicalOperator::WindowAggr { .. } => "WindowAggr",
            PhysicalOperator::Sort { .. } => "Sort",
            PhysicalOperator::Limit { .. } => "Limit",
            PhysicalOperator::ExchangeShuffle { .. } => "ExchangeShuffle",
            PhysicalOperator::ExchangeBroadcast => "ExchangeBroadcast",
            PhysicalOperator::Repartition => "Repartition",
            PhysicalOperator::Union { .. } => "Union",
            PhysicalOperator::Values => "Values",
        }
    }
}

/// What: Phase 2 physical plan root model.
///
/// Inputs:
/// - `operators`: Ordered operator pipeline.
/// - `sql`: Canonical SQL string used for explain and diagnostics.
/// - `schema_metadata`: Optional table column type contract for type coercion enforcement (Phase 9b+).
///
/// Output:
/// - Deterministic physical plan representation for query payload emission.
///
/// Details:
/// - schema_metadata is optional for backward compatibility with existing queries.
/// - When present, populated with ColumnDatatypeSpec entries mapping column names to type info.
/// - Used by Filter validator and worker runtime for strict type coercion policies.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalPlan {
    pub operators: Vec<PhysicalOperator>,
    pub sql: String,
    #[serde(default)]
    pub schema_metadata: Option<HashMap<String, ColumnDatatypeSpec>>,
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
