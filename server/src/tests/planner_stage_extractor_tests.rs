use crate::planner::stage_extractor::{StagePartitioningKind, extract_stages};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use std::sync::Arc;

/// What: Build a minimal empty plan node used as a deterministic leaf for extractor tests.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Empty physical plan with a one-column schema.
///
/// Details:
/// - This avoids provider/context setup and keeps tests focused on stage extraction.
fn build_empty_plan() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    Arc::new(EmptyExec::new(schema))
}

#[test]
fn extract_stages_returns_single_stage_without_repartition() {
    let stages = extract_stages(build_empty_plan());

    assert_eq!(stages.len(), 1);
    assert_eq!(stages[0].stage_id, 0);
    assert!(stages[0].input_stage_ids.is_empty());
    assert_eq!(stages[0].partitions_out, 1);
    assert_eq!(
        stages[0].output_partitioning.kind,
        StagePartitioningKind::Single
    );
}

#[test]
fn extract_stages_splits_one_repartition_boundary() {
    let root: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(build_empty_plan(), Partitioning::RoundRobinBatch(4))
            .expect("repartition plan should be constructible"),
    );

    let stages = extract_stages(root);

    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0].stage_id, 0);
    assert_eq!(stages[0].input_stage_ids, vec![1]);
    assert_eq!(stages[1].stage_id, 1);
    assert!(stages[1].input_stage_ids.is_empty());
    assert_eq!(stages[1].partitions_out, 4);
    assert_eq!(
        stages[1].output_partitioning.kind,
        StagePartitioningKind::RoundRobinBatch
    );
}

#[test]
fn extract_stages_handles_multiple_repartition_boundaries_in_order() {
    let lower: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(build_empty_plan(), Partitioning::UnknownPartitioning(3))
            .expect("lower repartition should be constructible"),
    );
    let root: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(lower, Partitioning::RoundRobinBatch(2))
            .expect("upper repartition should be constructible"),
    );

    let stages = extract_stages(root);

    assert_eq!(stages.len(), 3);
    assert_eq!(stages[0].input_stage_ids, vec![1]);
    assert_eq!(stages[1].input_stage_ids, vec![2]);
    assert!(stages[2].input_stage_ids.is_empty());

    assert_eq!(stages[1].partitions_out, 2);
    assert_eq!(
        stages[1].output_partitioning.kind,
        StagePartitioningKind::RoundRobinBatch
    );

    assert_eq!(stages[2].partitions_out, 3);
    assert_eq!(
        stages[2].output_partitioning.kind,
        StagePartitioningKind::Unknown
    );
}

#[test]
fn extract_stages_handles_branching_plan_with_single_repartition_branch() {
    let left: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(build_empty_plan(), Partitioning::RoundRobinBatch(5))
            .expect("left repartition should be constructible"),
    );
    let right = build_empty_plan();
    let root = UnionExec::try_new(vec![left, right]).expect("union should be constructible");

    let stages = extract_stages(root);

    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0].stage_id, 0);
    assert_eq!(stages[0].input_stage_ids, vec![1]);
    assert!(stages[0].node_names.iter().any(|name| name == "UnionExec"));

    assert_eq!(stages[1].stage_id, 1);
    assert!(stages[1].input_stage_ids.is_empty());
    assert_eq!(stages[1].partitions_out, 5);
    assert_eq!(
        stages[1].output_partitioning.kind,
        StagePartitioningKind::RoundRobinBatch
    );
}

#[test]
fn extract_stages_handles_branching_plan_with_two_repartition_branches() {
    let left: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(build_empty_plan(), Partitioning::RoundRobinBatch(2))
            .expect("left repartition should be constructible"),
    );
    let right: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(build_empty_plan(), Partitioning::UnknownPartitioning(7))
            .expect("right repartition should be constructible"),
    );
    let root = UnionExec::try_new(vec![left, right]).expect("union should be constructible");

    let stages = extract_stages(root);

    assert_eq!(stages.len(), 3);
    assert_eq!(stages[0].stage_id, 0);
    assert_eq!(stages[0].input_stage_ids, vec![1, 2]);

    assert_eq!(stages[1].stage_id, 1);
    assert!(stages[1].input_stage_ids.is_empty());
    assert_eq!(stages[1].partitions_out, 2);
    assert_eq!(
        stages[1].output_partitioning.kind,
        StagePartitioningKind::RoundRobinBatch
    );

    assert_eq!(stages[2].stage_id, 2);
    assert!(stages[2].input_stage_ids.is_empty());
    assert_eq!(stages[2].partitions_out, 7);
    assert_eq!(
        stages[2].output_partitioning.kind,
        StagePartitioningKind::Unknown
    );
}
