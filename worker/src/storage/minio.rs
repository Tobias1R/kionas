use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use aws_sdk_s3::config::endpoint::Endpoint as SmithyEndpoint;
use http::Uri;
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct MinioConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

impl MinioConfig {
    pub fn from_value(v: &Value) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bucket = v.get("bucket").and_then(|b| b.as_str()).ok_or("missing bucket")?.to_string();
        let region = v.get("region").and_then(|r| r.as_str()).unwrap_or("us-east-1").to_string();
        let endpoint = v.get("endpoint").and_then(|e| e.as_str()).map(|s| s.to_string());
        let access_key = v.get("access_key").and_then(|a| a.as_str()).map(|s| s.to_string());
        let secret_key = v.get("secret_key").and_then(|s| s.as_str()).map(|s| s.to_string());
        Ok(MinioConfig { bucket, region, endpoint, access_key, secret_key })
    }
}

#[derive(Clone)]
pub struct MinioProvider {
    client: Client,
    bucket: String,
}

impl MinioProvider {
    pub async fn new(cfg: MinioConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Load shared config, override region and endpoint if provided
        let region = Region::new(cfg.region.clone());

        // If explicit credentials were provided in cluster config, set them
        // as environment variables so `aws_config::from_env()` picks them up.
        if let (Some(access), Some(secret)) = (cfg.access_key.clone(), cfg.secret_key.clone()) {
            unsafe { std::env::set_var("AWS_ACCESS_KEY_ID", access.clone()); }
            unsafe { std::env::set_var("AWS_SECRET_ACCESS_KEY", secret.clone()); }
            if let Some(token) = cfg.access_key.as_ref() { // no-op if none
                let _ = token; // keep lint happy
            }
            log::info!("Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from Minio config");
        }

        let mut loader = aws_config::from_env().region(region.clone());

        let shared_config = loader.load().await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);

        if let Some(endpoint) = cfg.endpoint.as_ref() {
            // Custom endpoint handling is fragile across SDK versions; leave
            // it unset and log a warning so the user is aware. The SDK often
            // picks up endpoints correctly from environment or defaults and
            // the worker was observed to successfully store objects previously.
            // Configure explicit Minio endpoint and force path-style addressing
            // so buckets are addressed as `http://<host>:<port>/<bucket>/...`.
            // Try to create an SDK endpoint resolver from the provided string.
            // Parse provided endpoint string into an http::Uri and create
            // a smithy Endpoint via `immutable(Uri)`. Then set it as the
            // endpoint resolver and force path-style addressing so buckets
            // are addressed as `http://<host>:<port>/<bucket>/...`.
            // Set the explicit endpoint URL and force path-style addressing so
            // buckets are addressed as `http://<host>:<port>/<bucket>/...`.
            builder = builder.endpoint_url(endpoint.clone());
            builder = builder.force_path_style(true);
            log::info!("Configured Minio endpoint and enabled path-style: {}", endpoint);
        }

        let s3conf = builder.build();
        let client = Client::from_conf(s3conf);

        Ok(MinioProvider { client, bucket: cfg.bucket })
    }

    pub async fn put_object_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = ByteStream::from(bytes);
        match self.client.put_object().bucket(&self.bucket).key(key).body(body).send().await {
            Ok(_) => {
                log::debug!("S3 put_object succeeded for {}/{}", self.bucket, key);
                Ok(())
            }
            Err(e) => {
                // Log full debug information from the SDK error to aid diagnosis
                log::error!("S3 put_object failed for {}/{}: {:?}", self.bucket, key, e);
                return Err(Box::new(e));
            }
        }
    }

    pub async fn get_object_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self.client.get_object().bucket(&self.bucket).key(key).send().await;
        match resp {
            Ok(output) => {
                let data = output.body.collect().await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
                Ok(Some(data.into_bytes().to_vec()))
            }
            Err(e) => {
                // return Ok(None) if not found, otherwise propagate
                let msg = format!("{}", e);
                if msg.contains("NotFound") || msg.contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                }
            }
        }
    }

    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket).prefix(prefix);
            if let Some(t) = token.as_ref() { req = req.continuation_token(t); }
                let resp = req.send().await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
                let contents = resp.contents();
                for c in contents.iter() {
                    if let Some(k) = c.key() { keys.push(k.to_string().clone()); }
                }
            token = resp.next_continuation_token().map(|s| s.to_string());
            if token.is_none() { break; }
        }
        Ok(keys)
    }

    pub async fn delete_key(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client.delete_object().bucket(&self.bucket).key(key).send().await.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::storage::StorageProvider for MinioProvider {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.put_object_bytes(key, data).await
    }

    async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_object_bytes(key).await
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.list_keys(prefix).await
    }

    async fn delete_object(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.delete_key(key).await
    }
}
