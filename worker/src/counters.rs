use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[cfg(test)]
#[path = "tests/counters_tests.rs"]
mod tests;

/// What: Per-worker execution counters shared between service handlers and monitoring.
///
/// Inputs:
/// - Construct once at worker startup and share as `Arc<WorkerCounters>`.
///
/// Output:
/// - Exposes live counter fields and a snapshot view for monitoring serialization.
///
/// Details:
/// - Atomic operations use `Ordering::Relaxed` because counters are informational only.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    pub active_stages: AtomicU32,
    pub total_stages_executed: AtomicU64,
    pub active_partitions: AtomicU32,
    pub total_partitions_executed: AtomicU64,
    pub bytes_scanned_total: AtomicU64,
    pub total_stage_exec_ms: AtomicU64,
    pub total_rows_produced: AtomicU64,
}

impl WorkerCounters {
    /// What: Create a shared counter set initialized at zero.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - `Arc<WorkerCounters>` initialized for worker runtime usage.
    ///
    /// Details:
    /// - This constructor is intended for wiring into `SharedData`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// What: Read a point-in-time counter snapshot for monitoring payload assembly.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - `WorkerCounterSnapshot` with all worker execution counters.
    ///
    /// Details:
    /// - Loads are relaxed; values may change concurrently after this snapshot.
    pub fn snapshot(&self) -> WorkerCounterSnapshot {
        WorkerCounterSnapshot {
            active_stages: self.active_stages.load(Ordering::Relaxed),
            total_stages_executed: self.total_stages_executed.load(Ordering::Relaxed),
            active_partitions: self.active_partitions.load(Ordering::Relaxed),
            total_partitions_executed: self.total_partitions_executed.load(Ordering::Relaxed),
            bytes_scanned_total: self.bytes_scanned_total.load(Ordering::Relaxed),
            total_stage_exec_ms: self.total_stage_exec_ms.load(Ordering::Relaxed),
            total_rows_produced: self.total_rows_produced.load(Ordering::Relaxed),
        }
    }
}

/// What: Immutable snapshot of worker execution counters.
///
/// Inputs:
/// - Produced by `WorkerCounters::snapshot`.
///
/// Output:
/// - Copyable values suitable for serialization and logging.
///
/// Details:
/// - Keeps monitoring code free from direct atomic loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerCounterSnapshot {
    pub active_stages: u32,
    pub total_stages_executed: u64,
    pub active_partitions: u32,
    pub total_partitions_executed: u64,
    pub bytes_scanned_total: u64,
    pub total_stage_exec_ms: u64,
    pub total_rows_produced: u64,
}

/// What: RAII guard that tracks one active stage execution.
///
/// Inputs:
/// - `counters`: Shared worker counters.
///
/// Output:
/// - Guard instance that decrements active stages on drop.
///
/// Details:
/// - Drop also increments cumulative stage executions.
pub struct StageGuard(Arc<WorkerCounters>);

impl StageGuard {
    /// What: Acquire a stage guard and increment active stage count.
    ///
    /// Inputs:
    /// - `counters`: Shared worker counters.
    ///
    /// Output:
    /// - A guard value that must remain in scope for the stage lifetime.
    ///
    /// Details:
    /// - Uses relaxed atomics and clones the `Arc` for drop-time updates.
    pub fn acquire(counters: &Arc<WorkerCounters>) -> Self {
        counters.active_stages.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(counters))
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        self.0.active_stages.fetch_sub(1, Ordering::Relaxed);
        self.0.total_stages_executed.fetch_add(1, Ordering::Relaxed);
    }
}

/// What: RAII guard that tracks one active partition execution.
///
/// Inputs:
/// - `counters`: Shared worker counters.
///
/// Output:
/// - Guard instance that decrements active partitions on drop.
///
/// Details:
/// - Drop also increments cumulative partition executions.
pub struct PartitionGuard(Arc<WorkerCounters>);

impl PartitionGuard {
    /// What: Acquire a partition guard and increment active partition count.
    ///
    /// Inputs:
    /// - `counters`: Shared worker counters.
    ///
    /// Output:
    /// - A guard value that must remain in scope for the partition lifetime.
    ///
    /// Details:
    /// - Uses relaxed atomics and clones the `Arc` for drop-time updates.
    pub fn acquire(counters: &Arc<WorkerCounters>) -> Self {
        counters.active_partitions.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(counters))
    }
}

impl Drop for PartitionGuard {
    fn drop(&mut self) {
        self.0.active_partitions.fetch_sub(1, Ordering::Relaxed);
        self.0
            .total_partitions_executed
            .fetch_add(1, Ordering::Relaxed);
    }
}
