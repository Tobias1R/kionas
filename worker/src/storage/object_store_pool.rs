use crate::storage::object_store_adapter::object_store_from_config;
use deadpool::managed::{Manager, Metrics};
use serde_json::Value;
use std::sync::Arc;

/// What: Deadpool manager that creates and recycles S3/MinIO-backed ObjectStore clients.
///
/// Inputs:
/// - `storage`: Serialized storage configuration block from cluster settings.
///
/// Output:
/// - Manager that can produce pooled `Arc<dyn object_store::ObjectStore>` instances.
///
/// Details:
/// - The manager keeps an owned copy of storage config and uses the same
///   builder path as non-pooled code (`object_store_from_config`).
#[derive(Clone)]
pub struct ObjectStoreManager {
    storage: Value,
}

impl ObjectStoreManager {
    /// What: Construct a new manager from storage configuration.
    ///
    /// Inputs:
    /// - `storage`: Storage configuration from `cluster_info.storage`.
    ///
    /// Output:
    /// - `ObjectStoreManager` instance.
    ///
    /// Details:
    /// - The configuration is cloned once and reused for all pool creations.
    pub fn new(storage: &Value) -> Self {
        Self {
            storage: storage.clone(),
        }
    }
}

impl Manager for ObjectStoreManager {
    type Type = Arc<dyn object_store::ObjectStore>;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let storage = self.storage.clone();
        async move { object_store_from_config(&storage).await }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        // ObjectStore clients are stateless handles around pooled HTTP clients.
        Ok(())
    }
}
