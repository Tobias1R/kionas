use kionas::planner::{DistributedPhysicalPlan, PartitionSpec};
use serde::{Deserialize, Serialize};
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
    pub(crate) upstream_partition_counts: HashMap<u32, u32>,
    pub(crate) partition_spec: PartitionSpec,
    pub(crate) execution_mode_hint: ExecutionModeHint,
    pub(crate) output_destinations: Vec<OutputDestination>,
    pub(crate) operation: String,
    pub(crate) payload: String,
    pub(crate) params: HashMap<String, String>,
}

/// What: Stage execution mode hint for workers.
///
/// Inputs:
/// - Derived from distributed plan shape and stage fan-out.
///
/// Output:
/// - Contract hint that allows workers to differentiate local-only and distributed execution paths.
///
/// Details:
/// - Phase 1 computes this deterministically from stage graph metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionModeHint {
    LocalOnly,
    Distributed,
}

/// What: Downstream routing descriptor for one stage partition output.
///
/// Inputs:
/// - `downstream_stage_id`: Immediate downstream stage id.
/// - `worker_addresses`: Candidate worker addresses for downstream consumption.
/// - `partitioning`: Partitioning strategy expected by downstream stage.
/// - `downstream_partition_count`: Number of downstream partitions for routing.
///
/// Output:
/// - Deterministic routing metadata embedded in stage task params.
///
/// Details:
/// - Phase 1 emits metadata only; physical row transport is introduced in later phases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OutputDestination {
    pub(crate) downstream_stage_id: u32,
    pub(crate) worker_addresses: Vec<String>,
    pub(crate) partitioning: PartitionSpec,
    pub(crate) downstream_partition_count: u32,
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

fn resolve_execution_mode_hint(plan: &DistributedPhysicalPlan) -> ExecutionModeHint {
    if plan.stages.len() > 1
        || plan
            .stages
            .iter()
            .any(|stage| resolve_partition_count_for_stage(&stage.partition_spec) > 1)
    {
        ExecutionModeHint::Distributed
    } else {
        ExecutionModeHint::LocalOnly
    }
}

fn resolve_output_destinations(
    stage_id: u32,
    dependencies: &HashMap<u32, Vec<u32>>,
    partition_count_by_stage: &HashMap<u32, u32>,
    partition_spec_by_stage: &HashMap<u32, PartitionSpec>,
) -> Vec<OutputDestination> {
    let mut downstream = dependencies.get(&stage_id).cloned().unwrap_or_default();
    downstream.sort_unstable();

    downstream
        .into_iter()
        .map(|downstream_stage_id| {
            let downstream_partition_count = partition_count_by_stage
                .get(&downstream_stage_id)
                .copied()
                .unwrap_or(1);
            let partitioning = partition_spec_by_stage
                .get(&downstream_stage_id)
                .cloned()
                .unwrap_or(PartitionSpec::Single);

            OutputDestination {
                downstream_stage_id,
                worker_addresses: vec!["localhost".to_string()],
                partitioning,
                downstream_partition_count,
            }
        })
        .collect::<Vec<_>>()
}

/// What: Build topological stage waves for dependency-safe concurrent dispatch.
///
/// Inputs:
/// - `stage_groups`: Stage task groups containing stage ids and upstream dependencies.
///
/// Output:
/// - Ordered waves of stage ids where each wave can execute concurrently.
///
/// Details:
/// - Returns error when dependency graph is cyclic or contains no schedulable stage.
pub(crate) fn build_stage_execution_waves(
    stage_groups: &[StageTaskGroup],
) -> Result<Vec<Vec<u32>>, String> {
    let mut upstream_by_stage = HashMap::<u32, Vec<u32>>::new();
    for group in stage_groups {
        upstream_by_stage
            .entry(group.stage_id)
            .or_insert_with(|| group.upstream_stage_ids.clone());
    }

    let mut completed = std::collections::HashSet::<u32>::new();
    let mut waves = Vec::<Vec<u32>>::new();

    while completed.len() < upstream_by_stage.len() {
        let mut ready = upstream_by_stage
            .iter()
            .filter(|(stage_id, _)| !completed.contains(stage_id))
            .filter(|(_, deps)| deps.iter().all(|upstream| completed.contains(upstream)))
            .map(|(stage_id, _)| *stage_id)
            .collect::<Vec<_>>();
        ready.sort_unstable();

        if ready.is_empty() {
            return Err(
                "no schedulable stage found while building execution waves; dependency graph may be cyclic"
                    .to_string(),
            );
        }

        for stage_id in &ready {
            completed.insert(*stage_id);
        }
        waves.push(ready);
    }

    Ok(waves)
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
    let mut downstream_by_stage = HashMap::<u32, Vec<u32>>::new();
    for stage in &plan.stages {
        upstream_by_stage.insert(stage.stage_id, Vec::new());
        downstream_by_stage.insert(stage.stage_id, Vec::new());
    }

    for dep in &plan.dependencies {
        if let Some(entries) = upstream_by_stage.get_mut(&dep.to_stage_id) {
            entries.push(dep.from_stage_id);
        }
        if let Some(entries) = downstream_by_stage.get_mut(&dep.from_stage_id) {
            entries.push(dep.to_stage_id);
        }
    }

    let mut partition_count_by_stage = HashMap::<u32, u32>::new();
    let mut partition_spec_by_stage = HashMap::<u32, PartitionSpec>::new();
    for stage in &plan.stages {
        partition_count_by_stage.insert(
            stage.stage_id,
            resolve_partition_count_for_stage(&stage.partition_spec),
        );
        partition_spec_by_stage.insert(stage.stage_id, stage.partition_spec.clone());
    }

    let execution_mode_hint = resolve_execution_mode_hint(plan);

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

        let payload = serde_json::to_string(&stage.operators)
            .map_err(|e| format!("failed to serialize stage operators: {}", e))?;
        let output_destinations = resolve_output_destinations(
            stage.stage_id,
            &downstream_by_stage,
            &partition_count_by_stage,
            &partition_spec_by_stage,
        );
        for partition_index in 0..partition_count {
            let params = HashMap::new();

            groups.push(StageTaskGroup {
                stage_id: stage.stage_id,
                partition_index,
                partition_count,
                upstream_stage_ids: upstream.clone(),
                upstream_partition_counts: upstream_partition_counts.clone(),
                partition_spec: stage.partition_spec.clone(),
                execution_mode_hint: execution_mode_hint.clone(),
                output_destinations: output_destinations.clone(),
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
