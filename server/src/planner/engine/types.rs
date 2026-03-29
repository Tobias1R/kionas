/// What: Thin server-side wrapper for DataFusion planning operations.
///
/// Inputs:
/// - Optional relation providers passed per planning request.
///
/// Output:
/// - Consistent entrypoints for logical/physical diagnostics and Kionas translation.
///
/// Details:
/// - This wrapper consolidates planner calls used by query handlers so integration
///   points stay explicit during Phase 2 migration.
#[derive(Debug, Default, Clone, Copy)]
pub struct DataFusionQueryPlanner;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlannerIntentFlags {
    pub(crate) has_scan: bool,
    pub(crate) has_filter: bool,
    pub(crate) has_projection: bool,
    pub(crate) has_sort: bool,
    pub(crate) has_limit: bool,
    pub(crate) has_hash_join: bool,
    pub(crate) has_sort_merge_join: bool,
    pub(crate) has_nested_loop_join: bool,
    pub(crate) has_aggregate: bool,
    pub(crate) has_window: bool,
    pub(crate) has_union: bool,
    pub(crate) union_child_count: usize,
}

/// What: DataFusion planning artifacts generated from one SQL statement.
///
/// Inputs:
/// - Created by planning a SQL statement through DataFusion logical, optimizer, and physical phases.
///
/// Output:
/// - Text renderings for logical plan, optimized logical plan, and physical plan.
///
/// Details:
/// - Text artifacts are used for deterministic diagnostics while transport/runtime contracts are being migrated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionPlanArtifacts {
    pub logical_plan_text: String,
    pub optimized_logical_plan_text: String,
    pub physical_plan_text: String,
    pub stage_extraction: DataFusionStageExtractionDiagnostics,
}

/// What: Stage extraction diagnostics derived from the DataFusion physical plan.
///
/// Inputs:
/// - Output from server-side `extract_stages` traversal.
///
/// Output:
/// - Deterministic stage diagnostics attached to planning artifacts.
///
/// Details:
/// - This shape is consumed by query dispatch diagnostics and scheduling telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionStageExtractionDiagnostics {
    pub stage_count: usize,
    pub stages: Vec<DataFusionExtractedStage>,
}

/// What: One extracted stage diagnostic row.
///
/// Inputs:
/// - Stage metadata produced by extraction traversal.
///
/// Output:
/// - Compact diagnostics record for logs and payload diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionExtractedStage {
    pub stage_id: u32,
    pub input_stage_ids: Vec<u32>,
    pub partitions_out: usize,
    pub output_partitioning: String,
    pub output_partitioning_keys: Vec<String>,
    pub node_names: Vec<String>,
}
