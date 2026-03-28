use std::sync::atomic::Ordering;
use std::thread;

use crate::counters::{QueryGuard, QueryOutcome, ServerCounters};

#[test]
fn query_guard_defaults_to_failed_outcome() {
    let counters = ServerCounters::new();

    {
        let _guard = QueryGuard::acquire(&counters);
        assert_eq!(counters.active_queries.load(Ordering::Relaxed), 1);
        assert_eq!(counters.total_queries_submitted.load(Ordering::Relaxed), 1);
        assert_eq!(counters.total_queries_succeeded.load(Ordering::Relaxed), 0);
        assert_eq!(counters.total_queries_failed.load(Ordering::Relaxed), 0);
    }

    assert_eq!(counters.active_queries.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_queries_succeeded.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_queries_failed.load(Ordering::Relaxed), 1);
}

#[test]
fn query_guard_can_mark_success() {
    let counters = ServerCounters::new();

    {
        let mut guard = QueryGuard::acquire(&counters);
        guard.outcome = QueryOutcome::Succeeded;
    }

    assert_eq!(counters.active_queries.load(Ordering::Relaxed), 0);
    assert_eq!(counters.total_queries_submitted.load(Ordering::Relaxed), 1);
    assert_eq!(counters.total_queries_succeeded.load(Ordering::Relaxed), 1);
    assert_eq!(counters.total_queries_failed.load(Ordering::Relaxed), 0);
}

#[test]
fn concurrent_query_guards_preserve_submission_counts() {
    let counters = ServerCounters::new();
    let threads = 8_u32;
    let per_thread = 250_u32;

    let mut handles = Vec::new();
    for _ in 0..threads {
        let counters = counters.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..per_thread {
                let mut guard = QueryGuard::acquire(&counters);
                guard.outcome = QueryOutcome::Succeeded;
            }
        }));
    }

    for handle in handles {
        handle
            .join()
            .expect("query counter test thread should join");
    }

    let expected = u64::from(threads) * u64::from(per_thread);
    assert_eq!(counters.active_queries.load(Ordering::Relaxed), 0);
    assert_eq!(
        counters.total_queries_submitted.load(Ordering::Relaxed),
        expected
    );
    assert_eq!(
        counters.total_queries_succeeded.load(Ordering::Relaxed),
        expected
    );
    assert_eq!(counters.total_queries_failed.load(Ordering::Relaxed), 0);
}
