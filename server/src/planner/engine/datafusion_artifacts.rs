use crate::planner::engine::types::{
    DataFusionExtractedStage, DataFusionPlanArtifacts, DataFusionStageExtractionDiagnostics,
};
use crate::planner::stage_extractor::{StagePartitioningKind, extract_stages};
use crate::providers::{KionasRelationMetadata, build_session_context_with_kionas_providers};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::displayable;
use kionas::planner::PlannerError;
use std::sync::Arc;

/// What: Build a DataFusion logical plan and return a stable text rendering.
///
/// Inputs:
/// - `sql`: SQL statement to be parsed and planned by DataFusion.
///
/// Output:
/// - Human-readable logical plan text produced by DataFusion display APIs.
///
/// Details:
/// - This function is the server-side Objective 1 planning entrypoint.
/// - It intentionally uses DataFusion planning directly and does not involve custom logical models.
/// - Table/catalog resolution must be provided by the caller's configured DataFusion context in future objectives.
#[allow(dead_code)]
pub async fn build_datafusion_logical_plan_text(sql: &str) -> Result<String, PlannerError> {
    let context = SessionContext::new();
    let session_state = context.state();
    let logical_plan = session_state
        .create_logical_plan(sql)
        .await
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;

    Ok(logical_plan.display_indent_schema().to_string())
}

/// What: Build DataFusion logical, optimized logical, and physical plan diagnostics from SQL.
///
/// Inputs:
/// - `sql`: SQL statement to be parsed, optimized, and physically planned by DataFusion.
///
/// Output:
/// - Planning artifact bundle with text diagnostics for each stage.
///
/// Details:
/// - This function is the Objective 2 server planning entrypoint for physical planning diagnostics.
#[allow(dead_code)]
pub async fn build_datafusion_plan_artifacts(
    sql: &str,
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    build_datafusion_plan_artifacts_with_providers(sql, &[]).await
}

/// What: Build DataFusion planning artifacts using pre-registered relation providers.
///
/// Inputs:
/// - `sql`: SQL statement to plan.
/// - `relations`: Relation provider metadata used to register context tables.
///
/// Output:
/// - Planning artifact bundle with logical, optimized logical, and physical text diagnostics.
///
/// Details:
/// - Providers are built server-side from metastore metadata and avoid missing-table failures.
pub async fn build_datafusion_plan_artifacts_with_providers(
    sql: &str,
    relations: &[KionasRelationMetadata],
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    let context = build_session_context_with_kionas_providers(relations)
        .map_err(PlannerError::InvalidLogicalPlan)?;
    let session_state = context.state();
    let logical_plan = session_state
        .create_logical_plan(sql)
        .await
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let optimized_logical_plan = session_state
        .optimize(&logical_plan)
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let physical_plan = session_state
        .create_physical_plan(&optimized_logical_plan)
        .await
        .map_err(|err| PlannerError::InvalidPhysicalPipeline(err.to_string()))?;
    let extracted_stages = extract_stages(Arc::clone(&physical_plan));
    let stage_extraction = DataFusionStageExtractionDiagnostics {
        stage_count: extracted_stages.len(),
        stages: extracted_stages
            .into_iter()
            .map(|stage| {
                let output_partitioning = match stage.output_partitioning.kind {
                    StagePartitioningKind::Single => "single".to_string(),
                    StagePartitioningKind::RoundRobinBatch => "round_robin_batch".to_string(),
                    StagePartitioningKind::Hash => "hash".to_string(),
                    StagePartitioningKind::Unknown => "unknown".to_string(),
                };

                DataFusionExtractedStage {
                    stage_id: stage.stage_id,
                    input_stage_ids: stage.input_stage_ids,
                    partitions_out: stage.partitions_out,
                    output_partitioning,
                    output_partitioning_keys: stage.output_partitioning.hash_keys,
                    node_names: stage.node_names,
                }
            })
            .collect::<Vec<_>>(),
    };

    Ok(DataFusionPlanArtifacts {
        logical_plan_text: logical_plan.display_indent_schema().to_string(),
        optimized_logical_plan_text: optimized_logical_plan.display_indent_schema().to_string(),
        physical_plan_text: displayable(physical_plan.as_ref()).indent(true).to_string(),
        stage_extraction,
    })
}
