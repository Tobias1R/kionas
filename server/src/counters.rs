use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[cfg(test)]
#[path = "tests/counters_tests.rs"]
mod tests;

/// What: Server-level query counters shared between statement handlers and janitor.
///
/// Inputs:
/// - Construct once during server shared-state initialization and share via Arc.
///
/// Output:
/// - Provides atomic counters and `snapshot()` reads for dashboard publication.
///
/// Details:
/// - Uses `Ordering::Relaxed` because metrics are informational and not synchronization primitives.
#[derive(Debug, Default)]
pub struct ServerCounters {
    pub active_queries: AtomicU32,
    pub total_queries_submitted: AtomicU64,
    pub total_queries_succeeded: AtomicU64,
    pub total_queries_failed: AtomicU64,
}

impl ServerCounters {
    /// What: Create a shared server counter set initialized to zero.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - `Arc<ServerCounters>` ready for shared-state wiring.
    ///
    /// Details:
    /// - Preferred constructor for state initialization paths.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// What: Read a point-in-time snapshot of query counters.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - `ServerCounterSnapshot` with all server query counters.
    ///
    /// Details:
    /// - Values may change immediately after this call due to concurrent updates.
    pub fn snapshot(&self) -> ServerCounterSnapshot {
        ServerCounterSnapshot {
            active_queries: self.active_queries.load(Ordering::Relaxed),
            total_queries_submitted: self.total_queries_submitted.load(Ordering::Relaxed),
            total_queries_succeeded: self.total_queries_succeeded.load(Ordering::Relaxed),
            total_queries_failed: self.total_queries_failed.load(Ordering::Relaxed),
        }
    }
}

/// What: Immutable snapshot of server query counters.
///
/// Inputs:
/// - Produced by `ServerCounters::snapshot()`.
///
/// Output:
/// - Copyable values for janitor payload assembly and diagnostics.
///
/// Details:
/// - Keeps dashboard publishing logic independent from direct atomic loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerCounterSnapshot {
    pub active_queries: u32,
    pub total_queries_submitted: u64,
    pub total_queries_succeeded: u64,
    pub total_queries_failed: u64,
}

/// What: Outcome classification for one handled query statement.
///
/// Inputs:
/// - Set by statement handler before guard drop.
///
/// Output:
/// - Drives succeeded/failed cumulative counter updates.
///
/// Details:
/// - Default behavior in `QueryGuard::acquire` is `Failed` for safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOutcome {
    Succeeded,
    Failed,
}

/// What: RAII guard that tracks one in-flight query and records its final outcome.
///
/// Inputs:
/// - Acquired at query-dispatch entry using shared `ServerCounters`.
///
/// Output:
/// - On drop, decrements `active_queries` and increments success/failure totals.
///
/// Details:
/// - Defaults to `Failed` unless caller explicitly sets `outcome` to `Succeeded`.
pub struct QueryGuard {
    counters: Arc<ServerCounters>,
    pub outcome: QueryOutcome,
}

impl QueryGuard {
    /// What: Acquire a query guard and register query submission.
    ///
    /// Inputs:
    /// - `counters`: Shared server query counters.
    ///
    /// Output:
    /// - Guard value that should live for the query handling scope.
    ///
    /// Details:
    /// - Increments `active_queries` and `total_queries_submitted` eagerly.
    pub fn acquire(counters: &Arc<ServerCounters>) -> Self {
        counters.active_queries.fetch_add(1, Ordering::Relaxed);
        counters
            .total_queries_submitted
            .fetch_add(1, Ordering::Relaxed);
        Self {
            counters: Arc::clone(counters),
            outcome: QueryOutcome::Failed,
        }
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.counters.active_queries.fetch_sub(1, Ordering::Relaxed);
        match self.outcome {
            QueryOutcome::Succeeded => {
                self.counters
                    .total_queries_succeeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            QueryOutcome::Failed => {
                self.counters
                    .total_queries_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
