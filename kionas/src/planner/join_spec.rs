use crate::planner::logical_plan::LogicalRelation;
use serde::{Deserialize, Serialize};

/// What: Supported join type for the current roadmap slice.
///
/// Inputs:
/// - Variant value encoded by planner translation.
///
/// Output:
/// - Stable join type representation serialized in plan payloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
}

/// What: One join key pair used by equi-join operators.
///
/// Inputs:
/// - `left`: Left relation column expression.
/// - `right`: Right relation column expression.
///
/// Output:
/// - Serializable key pair for deterministic join contract checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinKeyPair {
    pub left: String,
    pub right: String,
}

/// What: Logical-level join specification carried by logical plans.
///
/// Inputs:
/// - `join_type`: Supported join type.
/// - `right_relation`: Right-side relation metadata.
/// - `keys`: Equi-join key pairs.
///
/// Output:
/// - Serializable logical join metadata consumed by physical translation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalJoinSpec {
    pub join_type: JoinType,
    pub right_relation: LogicalRelation,
    pub keys: Vec<JoinKeyPair>,
}

/// What: Structured join predicate expression carried by physical nested-loop joins.
///
/// Inputs:
/// - Variant payload captures equality/theta/composite join predicate shape.
///
/// Output:
/// - Serializable predicate contract consumed by worker join execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalJoinPredicate {
    Equality {
        left: String,
        right: String,
    },
    Theta {
        left: String,
        op: String,
        right: String,
    },
    Composite {
        predicates: Vec<PhysicalJoinPredicate>,
    },
}

/// What: Physical-level join specification carried by hash join operators.
///
/// Inputs:
/// - `join_type`: Supported join type.
/// - `right_relation`: Right-side relation used at runtime.
/// - `keys`: Equi-join key pairs.
///
/// Output:
/// - Serializable physical join metadata consumed by worker runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalJoinSpec {
    pub join_type: JoinType,
    pub right_relation: LogicalRelation,
    #[serde(default)]
    pub predicates: Vec<PhysicalJoinPredicate>,
    #[serde(default)]
    pub keys: Vec<JoinKeyPair>,
}
