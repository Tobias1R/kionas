#[cfg(test)]
mod flight_client_pool_tests {
    use crate::flight::client_pool::FlightClientPool;
    use std::time::Duration;

    #[tokio::test]
    async fn creates_pool_with_valid_config() {
        let pool = FlightClientPool::new(4, Duration::from_secs(5));
        assert_eq!(pool.max_cached_channels, 4);
    }

    #[tokio::test]
    async fn pool_get_channel_returns_error_for_unreachable_endpoint() {
        let pool = FlightClientPool::new(2, Duration::from_millis(100));
        let result = pool.get_channel("http://127.0.0.1:9999").await;

        assert!(result.is_err());
        match result {
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("timeout") || err_msg.contains("connection"),
                    "expected timeout or connection error, got: {}",
                    err_msg
                );
            }
            Ok(_) => panic!("should not succeed connecting to dead endpoint"),
        }
    }

    #[tokio::test]
    async fn pool_clear_all_empties_cache() {
        let pool = FlightClientPool::new(4, Duration::from_secs(5));
        pool.clear_all();
        // Cache should be empty after clear
        let channels = pool.channels.lock().unwrap();
        assert_eq!(channels.len(), 0);
    }
}
