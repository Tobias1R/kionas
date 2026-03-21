use crate::storage::minio::{MinioConfig, MinioProvider};
use deadpool::managed::Metrics;
use std::sync::Arc;

/// What: Deadpool manager that creates and recycles MinIO providers.
///
/// Inputs:
/// - `cfg`: MinIO configuration used to create each pooled provider instance.
///
/// Output:
/// - Manager capable of producing pooled `MinioProvider` values.
///
/// Details:
/// - Recycle is a no-op because the AWS SDK client encapsulates its own HTTP
///   connection pooling and remains valid across requests.
#[derive(Clone)]
pub struct MinioProviderManager {
    cfg: MinioConfig,
}

impl MinioProviderManager {
    /// What: Build a manager for pooled MinIO provider creation.
    ///
    /// Inputs:
    /// - `cfg`: MinIO configuration cloned into the manager.
    ///
    /// Output:
    /// - New manager instance.
    ///
    /// Details:
    /// - The manager is cheap to clone and stores immutable config only.
    pub fn new(cfg: MinioConfig) -> Self {
        Self { cfg }
    }
}

impl deadpool::managed::Manager for MinioProviderManager {
    type Type = MinioProvider;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let cfg = self.cfg.clone();
        async move { MinioProvider::new(cfg).await }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        Ok(())
    }
}

/// What: Storage provider wrapper that uses deadpool to checkout MinIO providers.
///
/// Inputs:
/// - `pool`: Deadpool instance used for provider checkout per request.
///
/// Output:
/// - Implements `StorageProvider` for pooled MinIO operations.
///
/// Details:
/// - This keeps query-path object operations bounded and observable under load.
#[derive(Clone)]
pub struct PooledMinioProvider {
    pool: Arc<deadpool::managed::Pool<MinioProviderManager>>,
}

impl PooledMinioProvider {
    /// What: Create a pooled MinIO storage provider.
    ///
    /// Inputs:
    /// - `cfg`: MinIO configuration for provider creation.
    /// - `pool_size`: Maximum number of pooled provider instances.
    ///
    /// Output:
    /// - `Ok(PooledMinioProvider)` when pool builds successfully.
    /// - `Err` with actionable details when pool construction fails.
    ///
    /// Details:
    /// - Pool size should be tuned based on query fan-out and remote storage latency.
    pub fn new(
        cfg: MinioConfig,
        pool_size: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let manager = MinioProviderManager::new(cfg);
        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(pool_size)
            .build()
            .map_err(|e| format!("failed to build minio provider pool: {}", e))?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// What: Checkout a provider instance from the pool.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Pooled provider object ready for one storage operation.
    ///
    /// Details:
    /// - Returns a boxed error to match `StorageProvider` trait signatures.
    async fn checkout(
        &self,
    ) -> Result<deadpool::managed::Object<MinioProviderManager>, Box<dyn std::error::Error + Send + Sync>>
    {
        self.pool
            .get()
            .await
            .map_err(|e| format!("minio provider checkout failed: {}", e).into())
    }
}

#[async_trait::async_trait]
impl crate::storage::StorageProvider for PooledMinioProvider {
    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = self.checkout().await?;
        provider.put_object_bytes(key, data).await
    }

    async fn get_object(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let provider = self.checkout().await?;
        provider.get_object_bytes(key).await
    }

    async fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let provider = self.checkout().await?;
        provider.list_keys(prefix).await
    }

    async fn delete_object(
        &self,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = self.checkout().await?;
        provider.delete_key(key).await
    }
}