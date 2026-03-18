use object_store::aws::AmazonS3Builder;
use serde_json::Value;
use std::sync::Arc;
// use object_store::ObjectStore; // unused import kept for future use

/// Build an `object_store::ObjectStore` from a storage config block.
///
/// Supports S3/MinIO-compatible configs with keys:
/// - `endpoint`, `bucket`, `region`, `access_key`, `secret_key`.
pub async fn object_store_from_config(
    storage: &Value,
) -> Result<Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = storage
        .get("endpoint")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let bucket = storage
        .get("bucket")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let region = storage
        .get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("us-east-1")
        .to_string();
    let access_key = storage
        .get("access_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let secret_key = storage
        .get("secret_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Allow HTTP for endpoints that start with http:// (MinIO in local dev)
    let allow_http = endpoint.starts_with("http://");

    let mut builder = AmazonS3Builder::new();
    if !region.is_empty() {
        builder = builder.with_region(region);
    }
    if !bucket.is_empty() {
        builder = builder.with_bucket_name(bucket);
    }
    if !access_key.is_empty() {
        builder = builder.with_access_key_id(access_key);
    }
    if !secret_key.is_empty() {
        builder = builder.with_secret_access_key(secret_key);
    }
    if !endpoint.is_empty() {
        builder = builder.with_endpoint(endpoint);
    }
    builder = builder.with_allow_http(allow_http);

    let store = builder.build()?;
    Ok(Arc::new(store))
}
