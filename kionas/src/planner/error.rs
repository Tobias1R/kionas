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
}

impl std::fmt::Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::EmptyProjection => write!(f, "logical plan projection cannot be empty"),
            PlannerError::EmptyRelation => write!(f, "logical plan relation cannot be empty"),
        }
    }
}

impl std::error::Error for PlannerError {}
