use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use std::collections::BTreeSet;
use std::sync::Arc;

/// What: Supported repartitioning kinds recognized for stage boundaries.
///
/// Inputs:
/// - Derived from DataFusion `Partitioning` variants on `RepartitionExec` nodes.
///
/// Output:
/// - Stable partitioning classification attached to extracted stages.
///
/// Details:
/// - This is intentionally narrow for Phase 1 control-plane extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagePartitioningKind {
    Single,
    RoundRobinBatch,
    Hash,
    Unknown,
}

/// What: Stage output partitioning descriptor captured from DataFusion exchange boundaries.
///
/// Inputs:
/// - One DataFusion `Partitioning` value from a `RepartitionExec` node.
///
/// Output:
/// - Serializable-friendly descriptor with type, count, and display rendering.
///
/// Details:
/// - `display` keeps a faithful textual representation for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageOutputPartitioning {
    pub kind: StagePartitioningKind,
    pub partition_count: usize,
    pub display: String,
    pub hash_keys: Vec<String>,
}

/// What: Extracted stage metadata derived from DataFusion physical plan traversal.
///
/// Inputs:
/// - Stage ids and dependencies discovered while traversing `ExecutionPlan` tree.
///
/// Output:
/// - Deterministic stage unit used by distributed DAG compilation and tests.
///
/// Details:
/// - A stage boundary is created at every `RepartitionExec` node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionStage {
    pub stage_id: u32,
    pub input_stage_ids: Vec<u32>,
    pub partitions_out: usize,
    pub output_partitioning: StageOutputPartitioning,
    pub node_names: Vec<String>,
}

/// What: Extract stage metadata by cutting a DataFusion physical plan at `RepartitionExec` boundaries.
///
/// Inputs:
/// - `root`: Physical plan root produced by DataFusion.
///
/// Output:
/// - Ordered stages with explicit upstream dependency ids.
///
/// Details:
/// - Stage `0` is always the root/downstream stage.
/// - Each encountered `RepartitionExec` introduces one upstream stage edge.
pub fn extract_stages(root: Arc<dyn ExecutionPlan>) -> Vec<ExecutionStage> {
    let mut stages = vec![ExecutionStage {
        stage_id: 0,
        input_stage_ids: Vec::new(),
        partitions_out: 1,
        output_partitioning: StageOutputPartitioning {
            kind: StagePartitioningKind::Single,
            partition_count: 1,
            display: "SinglePartition".to_string(),
            hash_keys: Vec::new(),
        },
        node_names: Vec::new(),
    }];

    let mut dependencies = BTreeSet::<(u32, u32)>::new();
    extract_stages_recursive(root, 0, &mut stages, &mut dependencies);

    for (from_stage_id, to_stage_id) in dependencies {
        if let Some(stage) = stages.get_mut(to_stage_id as usize) {
            stage.input_stage_ids.push(from_stage_id);
        }
    }

    for stage in &mut stages {
        stage.input_stage_ids.sort_unstable();
    }

    stages
}

/// What: Traverse execution-plan nodes and register stage boundaries at `RepartitionExec`.
///
/// Inputs:
/// - `plan`: Current DataFusion execution-plan node.
/// - `current_stage_id`: Downstream stage receiving this subtree.
/// - `stages`: Mutable extracted stages output.
/// - `dependencies`: Accumulated stage dependency edges.
///
/// Output:
/// - Mutates stage metadata and dependency graph in place.
///
/// Details:
/// - Repartition nodes are treated as exchange boundaries and are not added to
///   `node_names`; they define routing metadata for the upstream stage.
fn extract_stages_recursive(
    plan: Arc<dyn ExecutionPlan>,
    current_stage_id: u32,
    stages: &mut Vec<ExecutionStage>,
    dependencies: &mut BTreeSet<(u32, u32)>,
) {
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        let upstream_stage_id = stages.len() as u32;
        let output_partitioning = from_datafusion_partitioning(repartition.partitioning());
        stages.push(ExecutionStage {
            stage_id: upstream_stage_id,
            input_stage_ids: Vec::new(),
            partitions_out: output_partitioning.partition_count,
            output_partitioning,
            node_names: Vec::new(),
        });

        dependencies.insert((upstream_stage_id, current_stage_id));

        extract_stages_recursive(
            Arc::clone(repartition.input()),
            upstream_stage_id,
            stages,
            dependencies,
        );
        return;
    }

    if let Some(stage) = stages.get_mut(current_stage_id as usize) {
        stage.node_names.push(plan.name().to_string());
    }

    for child in plan.children() {
        extract_stages_recursive(Arc::clone(child), current_stage_id, stages, dependencies);
    }
}

/// What: Convert DataFusion partitioning metadata into stage extractor descriptor.
///
/// Inputs:
/// - `partitioning`: DataFusion output partitioning from one `RepartitionExec`.
///
/// Output:
/// - Stage partitioning descriptor with normalized kind and count.
///
/// Details:
/// - Hash expressions are intentionally not serialized in this phase; only
///   routing class and fan-out count are required.
fn from_datafusion_partitioning(partitioning: &Partitioning) -> StageOutputPartitioning {
    match partitioning {
        Partitioning::RoundRobinBatch(count) => StageOutputPartitioning {
            kind: StagePartitioningKind::RoundRobinBatch,
            partition_count: *count,
            display: partitioning.to_string(),
            hash_keys: Vec::new(),
        },
        Partitioning::Hash(expressions, count) => StageOutputPartitioning {
            kind: StagePartitioningKind::Hash,
            partition_count: *count,
            display: partitioning.to_string(),
            hash_keys: expressions
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        },
        Partitioning::UnknownPartitioning(count) => StageOutputPartitioning {
            kind: StagePartitioningKind::Unknown,
            partition_count: *count,
            display: partitioning.to_string(),
            hash_keys: Vec::new(),
        },
    }
}
