use crate::planner::logical_plan::LogicalExpr;
use crate::planner::physical_plan::PhysicalExpr;
use serde::{Deserialize, Serialize};

/// What: Supported aggregate functions for Phase 6 GROUP foundation.
///
/// Inputs:
/// - Variant value selected during projection and aggregate extraction.
///
/// Output:
/// - Stable function identifier serialized in logical/physical plan payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// What: One logical aggregate expression extracted from SELECT projection.
///
/// Inputs:
/// - `function`: Aggregate function kind.
/// - `input`: Optional input expression. `None` is reserved for `COUNT(*)`.
/// - `output_name`: Deterministic output column name used downstream.
///
/// Output:
/// - Serializable logical aggregate metadata consumed by physical translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalAggregateExpr {
    pub function: AggregateFunction,
    pub input: Option<LogicalExpr>,
    pub output_name: String,
}

/// What: Physical aggregate expression consumed by worker runtime.
///
/// Inputs:
/// - `function`: Aggregate function kind.
/// - `input`: Optional physical input expression. `None` is reserved for `COUNT(*)`.
/// - `output_name`: Deterministic output column name materialized in batch schema.
///
/// Output:
/// - Serializable physical aggregate metadata consumed by runtime aggregate operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalAggregateExpr {
    pub function: AggregateFunction,
    pub input: Option<PhysicalExpr>,
    pub output_name: String,
}

/// What: Shared physical aggregate operator specification.
///
/// Inputs:
/// - `grouping_exprs`: Grouping key expressions.
/// - `aggregates`: Aggregate expressions executed by partial/final stages.
///
/// Output:
/// - Serializable aggregate operator contract for planner/runtime integration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalAggregateSpec {
    pub grouping_exprs: Vec<PhysicalExpr>,
    pub aggregates: Vec<PhysicalAggregateExpr>,
}
