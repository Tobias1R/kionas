use kionas::planner::{DistributedPhysicalPlan, PartitionSpec};
use std::collections::HashMap;

/// What: Stage-scoped task group produced by distributed DAG compilation.
///
/// Inputs:
/// - `stage_id`: Stable stage identifier.
/// - `upstream_stage_ids`: Stage dependencies that must complete before execution.
/// - `operation`: Worker operation name for dispatch.
/// - `payload`: Stage-scoped serialized payload.
/// - `params`: Dispatch metadata map including stage and partition descriptors.
///
/// Output:
/// - Deterministic dispatch group used by stage schedulers.
///
/// Details:
/// - This model is additive and can be transformed into existing task dispatch requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageTaskGroup {
    pub(crate) stage_id: u32,
    pub(crate) partition_index: u32,
    pub(crate) partition_count: u32,
    pub(crate) upstream_stage_ids: Vec<u32>,
    pub(crate) operation: String,
    pub(crate) payload: String,
    pub(crate) params: HashMap<String, String>,
}

/// What: Resolve partition fan-out count for one distributed stage.
///
/// Inputs:
/// - `partition_spec`: Stage output partitioning strategy.
///
/// Output:
/// - Positive partition fan-out count used for task expansion.
fn resolve_partition_count_for_stage(partition_spec: &PartitionSpec) -> u32 {
    match partition_spec {
        PartitionSpec::Single | PartitionSpec::Broadcast => 1,
        PartitionSpec::Hash { .. } => std::env::var("KIONAS_STAGE_PARTITION_FANOUT")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .filter(|value| *value > 1)
            .or_else(|| {
                std::env::var("STAGE_PARTITION_FANOUT")
                    .ok()
                    .and_then(|raw| raw.parse::<u32>().ok())
                    .filter(|value| *value > 1)
            })
            .unwrap_or(4),
    }
}

/// What: Compile a distributed physical plan into stage task groups.
///
/// Inputs:
/// - `plan`: Distributed physical plan with stages and dependencies.
/// - `operation`: Worker operation to execute for each stage task group.
///
/// Output:
/// - Ordered stage task groups sorted by stage id.
/// - Error when payload serialization fails.
///
/// Details:
/// - Dependencies are encoded in params as JSON for compatibility with existing string-based task params.
pub(crate) fn compile_stage_task_groups(
    plan: &DistributedPhysicalPlan,
    operation: &str,
) -> Result<Vec<StageTaskGroup>, String> {
    let mut upstream_by_stage = HashMap::<u32, Vec<u32>>::new();
    for stage in &plan.stages {
        upstream_by_stage.insert(stage.stage_id, Vec::new());
    }

    for dep in &plan.dependencies {
        if let Some(entries) = upstream_by_stage.get_mut(&dep.to_stage_id) {
            entries.push(dep.from_stage_id);
        }
    }

    let mut partition_count_by_stage = HashMap::<u32, u32>::new();
    for stage in &plan.stages {
        partition_count_by_stage.insert(
            stage.stage_id,
            resolve_partition_count_for_stage(&stage.partition_spec),
        );
    }

    let mut groups = Vec::with_capacity(plan.stages.len());
    for stage in &plan.stages {
        let mut upstream = upstream_by_stage
            .get(&stage.stage_id)
            .cloned()
            .unwrap_or_default();
        upstream.sort_unstable();

        let partition_count = partition_count_by_stage
            .get(&stage.stage_id)
            .copied()
            .unwrap_or(1);
        let mut upstream_partition_counts = HashMap::<u32, u32>::new();
        for upstream_stage_id in &upstream {
            let upstream_partition_count = partition_count_by_stage
                .get(upstream_stage_id)
                .copied()
                .unwrap_or(1);
            upstream_partition_counts.insert(*upstream_stage_id, upstream_partition_count);
        }

        let upstream_serialized = serde_json::to_string(&upstream)
            .map_err(|e| format!("failed to serialize upstream ids: {}", e))?;
        let upstream_partition_counts_serialized =
            serde_json::to_string(&upstream_partition_counts)
                .map_err(|e| format!("failed to serialize upstream partition counts: {}", e))?;
        let partition_spec_serialized = serde_json::to_string(&stage.partition_spec)
            .map_err(|e| format!("failed to serialize partition spec: {}", e))?;
        let payload = serde_json::to_string(&stage.operators)
            .map_err(|e| format!("failed to serialize stage operators: {}", e))?;

        for partition_index in 0..partition_count {
            let mut params = HashMap::new();
            params.insert("stage_id".to_string(), stage.stage_id.to_string());
            params.insert(
                "upstream_stage_ids".to_string(),
                upstream_serialized.clone(),
            );
            params.insert(
                "upstream_partition_counts".to_string(),
                upstream_partition_counts_serialized.clone(),
            );
            params.insert(
                "partition_spec".to_string(),
                partition_spec_serialized.clone(),
            );
            params.insert("partition_count".to_string(), partition_count.to_string());
            params.insert("partition_index".to_string(), partition_index.to_string());

            groups.push(StageTaskGroup {
                stage_id: stage.stage_id,
                partition_index,
                partition_count,
                upstream_stage_ids: upstream.clone(),
                operation: operation.to_string(),
                payload: payload.clone(),
                params,
            });
        }
    }

    groups.sort_by_key(|group| (group.stage_id, group.partition_index));
    Ok(groups)
}

#[cfg(test)]
#[path = "../../tests/statement_handler_shared_distributed_dag_tests.rs"]
mod tests;
