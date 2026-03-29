mod datafusion_artifacts;
mod exec_intent;
mod fallback;
mod join_mapping;
mod plan_builder;
mod projection_parsing;
mod sql_intent;
mod translation_sources;
mod types;

use crate::providers::KionasRelationMetadata;
use kionas::planner::{PhysicalPlan, PlannerError};
use kionas::sql::query_model::SelectQueryModel;

pub use types::{
    DataFusionExtractedStage, DataFusionPlanArtifacts, DataFusionQueryPlanner,
    DataFusionStageExtractionDiagnostics,
};

#[cfg(test)]
pub(crate) use fallback::is_fallback_eligible_datafusion_error;

#[allow(dead_code)]
pub async fn build_datafusion_logical_plan_text(sql: &str) -> Result<String, PlannerError> {
    datafusion_artifacts::build_datafusion_logical_plan_text(sql).await
}

#[allow(dead_code)]
pub async fn build_datafusion_plan_artifacts(
    sql: &str,
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    datafusion_artifacts::build_datafusion_plan_artifacts(sql).await
}

pub async fn build_datafusion_plan_artifacts_with_providers(
    sql: &str,
    relations: &[KionasRelationMetadata],
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    datafusion_artifacts::build_datafusion_plan_artifacts_with_providers(sql, relations).await
}

#[allow(dead_code)]
pub async fn translate_datafusion_to_kionas_physical_plan(
    model: &SelectQueryModel,
) -> Result<PhysicalPlan, PlannerError> {
    plan_builder::translate_datafusion_to_kionas_physical_plan(model).await
}

pub async fn translate_datafusion_to_kionas_physical_plan_with_providers(
    model: &SelectQueryModel,
    relations: &[KionasRelationMetadata],
) -> Result<PhysicalPlan, PlannerError> {
    plan_builder::translate_datafusion_to_kionas_physical_plan_with_providers(model, relations)
        .await
}

impl DataFusionQueryPlanner {
    /// What: Construct a new DataFusion query planner wrapper.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Stateless planner wrapper instance.
    pub fn new() -> Self {
        Self
    }

    /// What: Build DataFusion planning diagnostics with relation providers.
    ///
    /// Inputs:
    /// - `sql`: Canonical SQL statement.
    /// - `relations`: Relation providers registered in DataFusion session context.
    ///
    /// Output:
    /// - Logical/optimized/physical diagnostics plus stage extraction metadata.
    pub async fn build_plan_artifacts(
        &self,
        sql: &str,
        relations: &[KionasRelationMetadata],
    ) -> Result<DataFusionPlanArtifacts, PlannerError> {
        build_datafusion_plan_artifacts_with_providers(sql, relations).await
    }

    /// What: Translate a canonical query model to Kionas physical operators.
    ///
    /// Inputs:
    /// - `model`: Canonical select query model.
    /// - `relations`: Relation providers for DataFusion planning.
    ///
    /// Output:
    /// - Kionas physical plan compatible with worker execution contract.
    pub async fn translate_to_kionas_plan(
        &self,
        model: &SelectQueryModel,
        relations: &[KionasRelationMetadata],
    ) -> Result<PhysicalPlan, PlannerError> {
        translate_datafusion_to_kionas_physical_plan_with_providers(model, relations).await
    }
}

#[cfg(test)]
#[path = "../../tests/planner_engine_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/spike_union_exec.rs"]
mod spike_union_exec;

#[cfg(test)]
#[path = "../../tests/phase4d_sprint1_integration_tests.rs"]
mod phase4d_sprint1_integration_tests;

#[cfg(test)]
#[path = "../../tests/phase4d_sprint2_integration_tests.rs"]
mod phase4d_sprint2_integration_tests;
