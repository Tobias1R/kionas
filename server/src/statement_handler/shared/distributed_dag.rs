use kionas::planner::{DistributedPhysicalPlan, DistributedStage, PartitionSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) const ROUTING_WORKER_SOURCE_PARAM: &str = "distributed_routing_worker_source";
pub(crate) const ROUTING_WORKER_COUNT_PARAM: &str = "distributed_routing_worker_count";
pub(crate) const ROUTING_RUNTIME_WORKER_COUNT_PARAM: &str =
    "distributed_routing_runtime_worker_count";
pub(crate) const ROUTING_EFFECTIVE_WORKER_COUNT_PARAM: &str =
    "distributed_routing_effective_worker_count";
pub(crate) const ROUTING_ENV_FALLBACK_APPLIED_PARAM: &str =
    "distributed_routing_env_fallback_applied";
pub(crate) const ROUTING_FALLBACK_KIND_PARAM: &str = "distributed_routing_fallback_kind";
pub(crate) const ROUTING_FALLBACK_ACTIVE_PARAM: &str = "distributed_routing_fallback_active";
pub(crate) const ROUTING_POOL_SOURCE_RUNTIME: &str = "runtime";
pub(crate) const ROUTING_POOL_SOURCE_ENV: &str = "env";
pub(crate) const ROUTING_POOL_SOURCE_DEFAULT: &str = "default";
pub(crate) const ROUTING_RUNTIME_SOURCE_WORKERS: &str = "workers";
pub(crate) const ROUTING_RUNTIME_SOURCE_WAREHOUSES: &str = "warehouses";
pub(crate) const ROUTING_RUNTIME_SOURCE_SESSION_POOL: &str = "session_pool";
pub(crate) const ROUTING_RUNTIME_SOURCE_FALLBACK: &str = "fallback";
pub(crate) const OBS_DAG_METRICS_JSON_PARAM: &str = "distributed_dag_metrics_json";
pub(crate) const OBS_PLAN_VALIDATION_STATUS_PARAM: &str = "distributed_plan_validation_status";
pub(crate) const OBS_PLAN_VALIDATION_STATUS_PASSED: &str = "passed";
pub(crate) const OBS_STAGE_EXTRACTION_MISMATCH_PARAM: &str = "stage_extraction_mismatch";
pub(crate) const OBS_DATAFUSION_STAGE_COUNT_PARAM: &str = "datafusion_stage_count";
pub(crate) const OBS_DISTRIBUTED_STAGE_COUNT_PARAM: &str = "distributed_stage_count";

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

/// What: Aggregated observability metrics for a compiled distributed DAG.
///
/// Inputs:
/// - Derived from one distributed physical plan and its compiled stage task groups.
///
/// Output:
/// - Serializable metrics payload for logs, diagnostics, and task params.
///
/// Details:
/// - Metrics are deterministic and computed from already materialized scheduler inputs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DistributedDagMetrics {
    pub(crate) stage_count: usize,
    pub(crate) dependency_count: usize,
    pub(crate) wave_count: usize,
    pub(crate) max_wave_width: usize,
    pub(crate) total_task_groups: usize,
    pub(crate) total_partitions: usize,
    pub(crate) max_partitions_per_stage: usize,
}

/// What: Resolve partition fan-out count for one distributed stage.
///
/// Inputs:
/// - `stage`: Distributed stage carrying partitioning strategy and optional explicit fan-out.
///
/// Output:
/// - Positive partition fan-out count used for task expansion.
fn resolve_partition_count_for_stage(stage: &DistributedStage) -> u32 {
    if let Some(explicit_count) = stage.output_partition_count
        && explicit_count > 0
    {
        return explicit_count;
    }

    match &stage.partition_spec {
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
            .any(|stage| resolve_partition_count_for_stage(stage) > 1)
    {
        ExecutionModeHint::Distributed
    } else {
        ExecutionModeHint::LocalOnly
    }
}

fn parse_worker_addresses(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    let parsed = if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<String>>(trimmed).unwrap_or_default()
    } else {
        trimmed
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };

    let mut deduped = Vec::<String>::new();
    for entry in parsed {
        let normalized = entry.trim().to_string();
        if normalized.is_empty() || deduped.iter().any(|existing| existing == &normalized) {
            continue;
        }
        deduped.push(normalized);
    }

    deduped
}

/// What: Resolve worker address pool used for downstream partition routing metadata.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Non-empty worker address pool in deterministic order.
///
/// Details:
/// - Uses `KIONAS_DISTRIBUTED_WORKER_ADDRESSES` when configured.
/// - Falls back to `localhost` to preserve existing behavior.
#[cfg(test)]
fn resolve_worker_address_pool() -> Vec<String> {
    resolve_worker_address_pool_with_source().0
}

fn resolve_worker_address_pool_with_source() -> (Vec<String>, &'static str) {
    let configured = std::env::var("KIONAS_DISTRIBUTED_WORKER_ADDRESSES")
        .ok()
        .map(|raw| parse_worker_addresses(&raw))
        .unwrap_or_default();

    if configured.is_empty() {
        (vec!["localhost".to_string()], ROUTING_POOL_SOURCE_DEFAULT)
    } else {
        (configured, ROUTING_POOL_SOURCE_ENV)
    }
}

/// What: Resolve the effective worker pool used for downstream routing metadata.
///
/// Inputs:
/// - `worker_pool`: Candidate runtime worker addresses supplied by caller context.
///
/// Output:
/// - Normalized, non-empty worker pool that will be used for routing decisions.
///
/// Details:
/// - Applies runtime normalization, then falls back to env/default pool when runtime input is empty.
pub(crate) fn resolve_effective_routing_worker_pool(worker_pool: &[String]) -> Vec<String> {
    resolve_effective_routing_worker_pool_with_source(worker_pool).0
}

/// What: Resolve effective routing worker pool and its source classification.
///
/// Inputs:
/// - `worker_pool`: Candidate runtime worker addresses supplied by caller context.
///
/// Output:
/// - Tuple with normalized non-empty worker pool and source label.
///
/// Details:
/// - Source is `runtime` when normalized runtime addresses are present.
/// - Source is `env` when fallback pool comes from configured env worker addresses.
/// - Source is `default` when fallback pool uses localhost default.
pub(crate) fn resolve_effective_routing_worker_pool_with_source(
    worker_pool: &[String],
) -> (Vec<String>, &'static str) {
    let mut normalized = Vec::<String>::new();
    for entry in worker_pool {
        let candidate = entry.trim().to_string();
        if candidate.is_empty() || normalized.iter().any(|existing| existing == &candidate) {
            continue;
        }
        normalized.push(candidate);
    }

    if normalized.is_empty() {
        resolve_worker_address_pool_with_source()
    } else {
        (normalized, ROUTING_POOL_SOURCE_RUNTIME)
    }
}

/// What: Build deterministic worker routing map for downstream partitions.
///
/// Inputs:
/// - `downstream_partition_count`: Number of downstream partitions to route.
/// - `worker_pool`: Candidate worker addresses.
///
/// Output:
/// - Ordered worker addresses where index aligns with downstream partition index.
///
/// Details:
/// - Applies round-robin assignment over the provided worker pool.
fn route_downstream_partitions_to_workers(
    downstream_partition_count: u32,
    worker_pool: &[String],
) -> Vec<String> {
    let normalized_pool = if worker_pool.is_empty() {
        vec!["localhost".to_string()]
    } else {
        worker_pool.to_vec()
    };

    log::info!(
        "route_downstream_partitions_to_workers: partitions={}, pool_size={}, pool={:?}",
        downstream_partition_count,
        normalized_pool.len(),
        normalized_pool
    );

    if downstream_partition_count == 0 {
        return Vec::new();
    }

    let result = (0..downstream_partition_count)
        .map(|partition_index| {
            let worker_index = (partition_index as usize) % normalized_pool.len();
            let address = normalized_pool[worker_index].clone();
            log::debug!(
                "route_downstream_partitions_to_workers: partition {} -> worker_index {} -> {}",
                partition_index,
                worker_index,
                address
            );
            address
        })
        .collect::<Vec<_>>();

    log::info!(
        "route_downstream_partitions_to_workers: generated addresses: {:?}",
        result
    );
    result
}

fn resolve_output_destinations(
    stage_id: u32,
    current_stage_partition_count: u32,
    dependencies: &HashMap<u32, Vec<u32>>,
    partition_count_by_stage: &HashMap<u32, u32>,
    partition_spec_by_stage: &HashMap<u32, PartitionSpec>,
    worker_pool: &[String],
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

            // Generate routing addresses for ALL upstream partitions, not just downstream
            // This ensures proper distribution when upstream > downstream
            let routing_partition_count =
                std::cmp::max(current_stage_partition_count, downstream_partition_count);
            log::info!(
                "resolve_output_destinations: stage_id={}, current_partitions={}, downstream_partitions={}, routing_count={}",
                stage_id,
                current_stage_partition_count,
                downstream_partition_count,
                routing_partition_count
            );

            let worker_addresses =
                route_downstream_partitions_to_workers(routing_partition_count, worker_pool);

            OutputDestination {
                downstream_stage_id,
                worker_addresses,
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

/// What: Compute observability metrics for one distributed DAG compilation result.
///
/// Inputs:
/// - `plan`: Distributed physical plan graph.
/// - `stage_groups`: Compiled stage task groups derived from `plan`.
///
/// Output:
/// - Deterministic DAG metrics including dependency, wave, and partition fan-out characteristics.
///
/// Details:
/// - Returns error only when stage wave derivation fails.
pub(crate) fn compute_distributed_dag_metrics(
    plan: &DistributedPhysicalPlan,
    stage_groups: &[StageTaskGroup],
) -> Result<DistributedDagMetrics, String> {
    let waves = build_stage_execution_waves(stage_groups)?;
    let mut partition_count_by_stage = HashMap::<u32, usize>::new();
    for group in stage_groups {
        partition_count_by_stage
            .entry(group.stage_id)
            .or_insert(group.partition_count as usize);
    }

    let max_wave_width = waves.iter().map(std::vec::Vec::len).max().unwrap_or(0);
    let max_partitions_per_stage = partition_count_by_stage
        .values()
        .copied()
        .max()
        .unwrap_or(0);

    Ok(DistributedDagMetrics {
        stage_count: plan.stages.len(),
        dependency_count: plan.dependencies.len(),
        wave_count: waves.len(),
        max_wave_width,
        total_task_groups: stage_groups.len(),
        total_partitions: partition_count_by_stage.values().sum::<usize>(),
        max_partitions_per_stage,
    })
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
#[cfg(test)]
pub(crate) fn compile_stage_task_groups(
    plan: &DistributedPhysicalPlan,
    operation: &str,
) -> Result<Vec<StageTaskGroup>, String> {
    let worker_pool = resolve_worker_address_pool();
    compile_stage_task_groups_with_worker_pool(plan, operation, &worker_pool)
}

/// What: Compile a distributed physical plan into stage task groups using an explicit worker pool.
///
/// Inputs:
/// - `plan`: Distributed physical plan with stages and dependencies.
/// - `operation`: Worker operation to execute for each stage task group.
/// - `worker_pool`: Candidate worker addresses used for output destination routing metadata.
///
/// Output:
/// - Ordered stage task groups sorted by stage id.
/// - Error when payload serialization fails.
///
/// Details:
/// - Runtime worker pools can be passed from cluster/session state.
/// - Falls back to env/default worker pool when runtime pool is empty.
pub(crate) fn compile_stage_task_groups_with_worker_pool(
    plan: &DistributedPhysicalPlan,
    operation: &str,
    worker_pool: &[String],
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
        partition_count_by_stage.insert(stage.stage_id, resolve_partition_count_for_stage(stage));
        partition_spec_by_stage.insert(stage.stage_id, stage.partition_spec.clone());
    }

    let execution_mode_hint = resolve_execution_mode_hint(plan);
    let normalized_worker_pool = resolve_effective_routing_worker_pool(worker_pool);

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
            partition_count,
            &downstream_by_stage,
            &partition_count_by_stage,
            &partition_spec_by_stage,
            &normalized_worker_pool,
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
