use crate::planner::engine::types::PlannerIntentFlags;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

fn collect_exec_node_names(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    out.push(plan.name().to_string());
    for child in plan.children() {
        collect_exec_node_names(child, out);
    }
}

fn collect_union_child_counts(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
    if plan.name().to_ascii_lowercase().contains("unionexec") {
        out.push(plan.children().len());
    }

    for child in plan.children() {
        collect_union_child_counts(child, out);
    }
}

/// What: Detect join execution node families present in a DataFusion physical plan tree.
///
/// Inputs:
/// - `node_names`: Flattened execution node names collected from DataFusion plan tree.
///
/// Output:
/// - Tuple flags in order: `(hash_join, sort_merge_join, nested_loop_join)`.
///
/// Details:
/// - This supports explicit contract checks so unsupported join families fail fast.
fn detect_join_families(node_names: &[String]) -> (bool, bool, bool) {
    let has_hash_join = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("hashjoin"));
    let has_sort_merge_join = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("sortmergejoin"));
    let has_nested_loop_join = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("nestedloopjoin") || lower.contains("crossjoin")
    });

    (has_hash_join, has_sort_merge_join, has_nested_loop_join)
}

/// What: Derive planner intent flags from DataFusion physical-plan node names.
///
/// Inputs:
/// - `plan`: DataFusion execution plan root.
///
/// Output:
/// - Normalized intent flags used by Kionas operator translation.
///
/// Details:
/// - This keeps translation decisions tied to DataFusion physical planning when available.
pub(crate) fn derive_intent_from_exec_plan(plan: &Arc<dyn ExecutionPlan>) -> PlannerIntentFlags {
    let mut node_names = Vec::new();
    collect_exec_node_names(plan, &mut node_names);

    let has_scan = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("datasource")
            || lower.contains("parquet")
            || lower.contains("csv")
            || lower.contains("memoryexec")
    });
    let has_filter = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("filterexec"));
    let has_projection = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("projectionexec"));
    let has_sort = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("sortexec") || lower.contains("sortpreservingmergeexec")
    });
    let has_limit = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("globallimitexec") || lower.contains("locallimitexec")
    });
    let (has_hash_join, has_sort_merge_join, has_nested_loop_join) =
        detect_join_families(&node_names);
    let has_window = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("window")
    });
    let has_aggregate = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("aggregateexec") && !lower.contains("window")
    });
    let mut union_child_counts = Vec::<usize>::new();
    collect_union_child_counts(plan, &mut union_child_counts);
    let has_union = !union_child_counts.is_empty();
    let union_child_count = union_child_counts.into_iter().max().unwrap_or(0);

    PlannerIntentFlags {
        has_scan,
        has_filter,
        has_projection,
        has_sort,
        has_limit,
        has_hash_join,
        has_sort_merge_join,
        has_nested_loop_join,
        has_aggregate,
        has_window,
        has_union,
        union_child_count,
    }
}
