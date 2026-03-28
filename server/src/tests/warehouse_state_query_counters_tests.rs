use std::sync::atomic::Ordering;

use crate::warehouse::state::SharedState;
use kionas::config::{AppConfig, LoggingConfig, ServicesConfig};

#[test]
fn shared_state_default_initializes_query_counters() {
    let shared_state = SharedState::default();
    let snapshot = shared_state.query_counters.snapshot();

    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.total_queries_submitted, 0);
    assert_eq!(snapshot.total_queries_succeeded, 0);
    assert_eq!(snapshot.total_queries_failed, 0);
}

#[test]
fn shared_state_new_initializes_query_counters() {
    let config = AppConfig {
        mode: "server".to_string(),
        consul_host: String::new(),
        logging: LoggingConfig {
            level: "info".to_string(),
            format: "text".to_string(),
            output: "stdout".to_string(),
        },
        services: ServicesConfig {
            security: None,
            interops: None,
            warehouse: None,
            postgres: None,
        },
        session: None,
        cleanup: None,
    };
    let shared_state = SharedState::new(config);

    assert_eq!(
        shared_state
            .query_counters
            .active_queries
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        shared_state
            .query_counters
            .total_queries_submitted
            .load(Ordering::Relaxed),
        0
    );
}
