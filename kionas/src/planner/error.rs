/// What: Planner error variants for Phase 1 logical planning.
///
/// Inputs:
/// - Variants capture deterministic translation and validation failures.
///
/// Output:
/// - Typed planner errors suitable for validation-grade responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    EmptyProjection,
    EmptyRelation,
    InvalidLogicalPlan(String),
    EmptyPhysicalPlan,
    InvalidPhysicalPipeline(String),
    InvalidDistributedPlan(String),
    UnsupportedPhysicalOperator(String),
    UnsupportedPredicate(String),
}

impl std::fmt::Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::EmptyProjection => write!(f, "logical plan projection cannot be empty"),
            PlannerError::EmptyRelation => write!(f, "logical plan relation cannot be empty"),
            PlannerError::InvalidLogicalPlan(message) => {
                write!(f, "invalid logical plan: {}", message)
            }
            PlannerError::EmptyPhysicalPlan => {
                write!(f, "physical plan operator list cannot be empty")
            }
            PlannerError::InvalidPhysicalPipeline(message) => {
                write!(f, "invalid physical pipeline: {}", message)
            }
            PlannerError::InvalidDistributedPlan(message) => {
                write!(f, "invalid distributed plan: {}", message)
            }
            PlannerError::UnsupportedPhysicalOperator(name) => {
                write!(
                    f,
                    "physical operator '{}' is not supported in this phase",
                    name
                )
            }
            PlannerError::UnsupportedPredicate(message) => {
                write!(f, "predicate is not supported in this phase: {}", message)
            }
        }
    }
}

impl std::error::Error for PlannerError {}
