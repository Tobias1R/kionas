use std::sync::atomic::Ordering;
use std::thread;

use crate::counters::{PartitionGuard, StageGuard, WorkerCounters};

#[test]
fn stage_guard_drop_updates_stage_counters() {
    let counters = WorkerCounters::new();

    {
        let _guard = StageGuard::acquire(&counters);
        assert_eq!(counters.active_stages.load(Ordering::Relaxed), 1);
        assert_eq!(counters.total_stages_executed.load(Ordering::Relaxed), 0);
    }

    assert_eq!(counters.active_stages.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_stages_executed.load(Ordering::Relaxed), 1);
}

#[test]
fn partition_guard_drop_updates_partition_counters() {
    let counters = WorkerCounters::new();

    {
        let _guard = PartitionGuard::acquire(&counters);
        assert_eq!(counters.active_partitions.load(Ordering::Relaxed), 1);
        assert_eq!(
            counters.total_partitions_executed.load(Ordering::Relaxed),
            0
        );
    }

    assert_eq!(counters.active_partitions.load(Ordering::Relaxed), 0);
    assert_eq!(
        counters.total_partitions_executed.load(Ordering::Relaxed),
        1
    );
}

#[test]
fn concurrent_guard_updates_are_not_lost() {
    let counters = WorkerCounters::new();
    let iterations_per_thread = 300_u32;
    let thread_count = 8_u32;

    let mut threads = Vec::new();
    for _ in 0..thread_count {
        let counters = counters.clone();
        threads.push(thread::spawn(move || {
            for _ in 0..iterations_per_thread {
                let _stage_guard = StageGuard::acquire(&counters);
                let _partition_guard = PartitionGuard::acquire(&counters);
            }
        }));
    }

    for handle in threads {
        handle
            .join()
            .expect("counter test worker thread should join");
    }

    let expected_total = u64::from(iterations_per_thread) * u64::from(thread_count);
    assert_eq!(counters.active_stages.load(Ordering::Relaxed), 0);
    assert_eq!(
        counters.total_stages_executed.load(Ordering::Relaxed),
        expected_total
    );
    assert_eq!(counters.active_partitions.load(Ordering::Relaxed), 0);
    assert_eq!(
        counters.total_partitions_executed.load(Ordering::Relaxed),
        expected_total
    );
}

#[test]
fn cumulative_stage_metric_counters_start_at_zero() {
    let counters = WorkerCounters::new();

    assert_eq!(counters.bytes_scanned_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_stage_exec_ms.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_rows_produced.load(Ordering::Relaxed), 0);
}

#[test]
fn snapshot_includes_extended_stage_metric_counters() {
    let counters = WorkerCounters::new();

    counters
        .bytes_scanned_total
        .fetch_add(1_024, Ordering::Relaxed);
    counters
        .total_stage_exec_ms
        .fetch_add(2_500, Ordering::Relaxed);
    counters
        .total_rows_produced
        .fetch_add(333, Ordering::Relaxed);

    let snapshot = counters.snapshot();
    assert_eq!(snapshot.bytes_scanned_total, 1_024);
    assert_eq!(snapshot.total_stage_exec_ms, 2_500);
    assert_eq!(snapshot.total_rows_produced, 333);
}
