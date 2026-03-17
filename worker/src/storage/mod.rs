use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "storage-minio")]
pub mod minio;

#[async_trait]
pub trait StorageProvider: Send + Sync {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;
    async fn delete_object(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub async fn build_provider_from_cluster(storage: &Value) -> Result<Arc<dyn StorageProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let storage_type = storage.get("storage_type").and_then(|v| v.as_str()).unwrap_or("minio");
    match storage_type {
        "minio" => {
            #[cfg(feature = "storage-minio")]
            {
                let cfg = minio::MinioConfig::from_value(storage)?;
                let prov = minio::MinioProvider::new(cfg).await?;
                return Ok(Arc::new(prov));
            }
            #[cfg(not(feature = "storage-minio"))]
            {
                return Err("minio support not compiled into this binary (feature \"storage-minio\" missing)".into());
            }
        }
        other => Err(format!("Unsupported storage type: {}", other).into()),
    }
}
