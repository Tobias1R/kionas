mod non_union_builder;
mod union_builder;

use crate::planner::engine::exec_intent::derive_intent_from_exec_plan;
use crate::planner::engine::fallback::{
    is_fallback_eligible_datafusion_error, is_object_store_registration_error,
    requires_strict_datafusion_planning,
};
use crate::planner::engine::join_mapping::cte_projection_source_map;
use crate::planner::engine::sql_intent::derive_intent_from_sql_text;
use crate::planner::engine::translation_sources::{
    collect_translation_joins, collect_translation_source_chain, deepest_translation_source_model,
    translation_source_model,
};
use crate::planner::engine::types::PlannerIntentFlags;
use crate::planner::stage_extractor::extract_stages;
use crate::providers::{KionasRelationMetadata, build_session_context_with_kionas_providers};
use kionas::planner::{LogicalRelation, PhysicalPlan, PlannerError};
use kionas::sql::query_model::{SelectQueryModel, primary_relation_dependency};
use std::sync::Arc;

/// What: Translate DataFusion-driven planning intent into Kionas physical operator plan.
///
/// Inputs:
/// - `model`: Canonical SELECT query model used by server dispatch pipeline.
///
/// Output:
/// - Kionas physical plan used for worker task operator payloads.
///
/// Details:
/// - DataFusion remains the source of planning diagnostics and optimization intent.
/// - This translation boundary preserves the worker contract: Kionas operator tasks only.
/// - Current implementation reuses existing Kionas logical->physical conversion while the
///   DataFusion physical-plan-to-operator mapping is incrementally implemented.
#[allow(dead_code)]
pub async fn translate_datafusion_to_kionas_physical_plan(
    model: &SelectQueryModel,
) -> Result<PhysicalPlan, PlannerError> {
    translate_datafusion_to_kionas_physical_plan_with_providers(model, &[]).await
}

/// What: Translate DataFusion-driven planning intent into Kionas operators with providers.
///
/// Inputs:
/// - `model`: Canonical SELECT query model used by server dispatch pipeline.
/// - `relations`: Relation provider metadata used to initialize DataFusion context.
///
/// Output:
/// - Kionas physical plan used for worker task operator payloads.
///
/// Details:
/// - Fallback intent derivation remains as a graceful safeguard when planning fails.
pub async fn translate_datafusion_to_kionas_physical_plan_with_providers(
    model: &SelectQueryModel,
    relations: &[KionasRelationMetadata],
) -> Result<PhysicalPlan, PlannerError> {
    let context = build_session_context_with_kionas_providers(relations)
        .map_err(PlannerError::InvalidLogicalPlan)?;
    let session_state = context.state();
    let logical_plan = match session_state.create_logical_plan(&model.sql).await {
        Ok(plan) => plan,
        Err(err) => {
            let message = err.to_string();
            if is_fallback_eligible_datafusion_error(&message) {
                if requires_strict_datafusion_planning(model)
                    && !is_object_store_registration_error(&message)
                {
                    return Err(PlannerError::InvalidLogicalPlan(format!(
                        "subquery/CTE query requires DataFusion planning and cannot use SQL-text fallback: {}",
                        message
                    )));
                }
                let intent = derive_intent_from_sql_text(&model.sql);
                return build_kionas_plan_from_intent(model, intent);
            }

            return Err(PlannerError::InvalidLogicalPlan(message));
        }
    };
    let optimized_logical_plan = session_state
        .optimize(&logical_plan)
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let physical_plan = match session_state
        .create_physical_plan(&optimized_logical_plan)
        .await
    {
        Ok(plan) => plan,
        Err(err) => {
            let message = err.to_string();
            if is_fallback_eligible_datafusion_error(&message) {
                if requires_strict_datafusion_planning(model)
                    && !is_object_store_registration_error(&message)
                {
                    return Err(PlannerError::InvalidPhysicalPipeline(format!(
                        "subquery/CTE query requires DataFusion physical planning and cannot use SQL-text fallback: {}",
                        message
                    )));
                }
                let intent = derive_intent_from_sql_text(&model.sql);
                return build_kionas_plan_from_intent(model, intent);
            }

            return Err(PlannerError::InvalidPhysicalPipeline(message));
        }
    };

    let intent = derive_intent_from_exec_plan(&physical_plan);
    let extracted_stages = extract_stages(Arc::clone(&physical_plan));
    log::debug!(
        "datafusion stage extraction completed with {} stages",
        extracted_stages.len()
    );
    build_kionas_plan_from_intent(model, intent)
}

/// What: Dispatch translation into union and non-union physical-plan builders.
///
/// Inputs:
/// - `model`: Canonical query model.
/// - `intent`: Planner intent flags derived from DataFusion.
///
/// Output:
/// - Validated Kionas physical plan.
///
/// Details:
/// - Shared translation context is prepared once and reused across both builder paths.
fn build_kionas_plan_from_intent(
    model: &SelectQueryModel,
    intent: PlannerIntentFlags,
) -> Result<PhysicalPlan, PlannerError> {
    let source_model = translation_source_model(model);
    let source_chain = collect_translation_source_chain(model);
    let translation_joins = collect_translation_joins(model, &source_chain);
    let cte_sources = cte_projection_source_map(source_model);
    let scan_source_model = deepest_translation_source_model(model);
    let primary_relation = primary_relation_dependency(scan_source_model).ok_or_else(|| {
        PlannerError::InvalidPhysicalPipeline(
            "query model has no relation dependencies for table scan translation".to_string(),
        )
    })?;

    let scan_relation = LogicalRelation {
        database: primary_relation.database.clone(),
        schema: primary_relation.schema.clone(),
        table: primary_relation.table.clone(),
    };

    if intent.has_union {
        return union_builder::build_union_plan(model, intent, &scan_relation);
    }

    non_union_builder::build_non_union_plan(
        model,
        intent,
        source_model,
        &source_chain,
        &translation_joins,
        &cte_sources,
        &scan_relation,
    )
}
