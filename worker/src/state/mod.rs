use crate::interops::manager::InteropsManager;
use crate::storage::StorageProvider;
use crate::storage::object_store_pool::ObjectStoreManager;
use deadpool::managed::Pool;
use kionas::constants::{
    REDIS_DB_STATUS, REDIS_POOL_SIZE_ENV, REDIS_TASK_RESULT_KEY_PREFIX,
    REDIS_TASK_RESULT_TTL_SECONDS, REDIS_URL_ENV,
};
use kionas::consul::ClusterInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type RedisPool = Arc<Pool<RedisConnectionManager>>;

pub(crate) struct RedisConnectionManager {
    redis_url: String,
}

impl deadpool::managed::Manager for RedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Type, Self::Error>> + Send>> {
        let redis_url = self.redis_url.clone();
        Box::pin(async move {
            let client = redis::Client::open(redis_url)?;
            let conn = client.get_multiplexed_async_connection().await?;
            Ok(conn)
        })
    }

    fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), deadpool::managed::RecycleError<Self::Error>>> + Send>,
    > {
        Box::pin(async move { Ok(()) })
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug)]
pub struct WorkerInformation {
    pub worker_id: String,
    pub host: String,
    pub port: u32,
    pub server_url: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub ca_cert_path: String,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct SharedData {
    pub worker_info: WorkerInformation,
    pub cluster_info: ClusterInfo,
    #[serde(skip)]
    pub storage_provider: Option<Arc<dyn StorageProvider + Send + Sync>>,
    #[serde(skip)]
    pub master_pool: Arc<tokio::sync::Mutex<Option<Arc<Pool<InteropsManager>>>>>,
    #[serde(skip)]
    pub object_store_pool: Arc<tokio::sync::Mutex<Option<Arc<Pool<ObjectStoreManager>>>>>,
    #[serde(skip)]
    pub task_result_locations: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
    #[serde(skip)]
    pub redis_pool: Arc<tokio::sync::Mutex<Option<RedisPool>>>,
}

impl fmt::Debug for SharedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedData")
            .field("worker_info", &self.worker_info)
            .field("cluster_info", &self.cluster_info)
            .finish()
    }
}

impl SharedData {
    pub fn new(worker_info: WorkerInformation, cluster_info: ClusterInfo) -> Self {
        SharedData {
            worker_info,
            cluster_info,
            storage_provider: None,
            master_pool: Arc::new(tokio::sync::Mutex::new(None)),
            object_store_pool: Arc::new(tokio::sync::Mutex::new(None)),
            task_result_locations: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            redis_pool: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn set_storage_provider(&mut self, prov: Arc<dyn StorageProvider + Send + Sync>) {
        self.storage_provider = Some(prov);
    }

    pub fn set_master_pool(&mut self, pool: Arc<Pool<InteropsManager>>) {
        // best-effort synchronous set for initialization paths
        let mutex = self.master_pool.clone();
        // spawn a task to set it since we can't block here
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            let mut guard = mutex.lock().await;
            *guard = Some(pool_clone);
        });
    }

    fn build_task_result_key(&self, session_id: &str, task_id: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            REDIS_TASK_RESULT_KEY_PREFIX, self.worker_info.worker_id, session_id, task_id
        )
    }

    fn default_redis_url() -> String {
        format!("redis://redis:6379/{}", REDIS_DB_STATUS)
    }

    async fn ensure_redis_pool(&self) -> Option<RedisPool> {
        let mut guard = self.redis_pool.lock().await;
        if guard.is_none() {
            let redis_url =
                std::env::var(REDIS_URL_ENV).unwrap_or_else(|_| Self::default_redis_url());
            let pool_size = std::env::var(REDIS_POOL_SIZE_ENV)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(8);

            let manager = RedisConnectionManager { redis_url };
            match deadpool::managed::Pool::builder(manager)
                .max_size(pool_size)
                .build()
            {
                Ok(pool) => {
                    let pool = Arc::new(pool);
                    *guard = Some(pool.clone());
                    Some(pool)
                }
                Err(e) => {
                    log::error!("failed to build redis pool: {}", e);
                    None
                }
            }
        } else {
            guard.as_ref().cloned()
        }
    }

    pub async fn set_task_result_location(
        &self,
        session_id: &str,
        task_id: &str,
        result_location: &str,
    ) {
        let session_id = session_id.trim();
        let task_id = task_id.trim();
        let result_location = result_location.trim();
        if session_id.is_empty() || task_id.is_empty() || result_location.is_empty() {
            return;
        }

        let cache_key = format!("{}:{}", session_id, task_id);
        let mut guard = self.task_result_locations.write().await;
        guard.insert(cache_key.clone(), result_location.to_string());
        drop(guard);

        if let Some(pool) = self.ensure_redis_pool().await {
            match pool.get().await {
                Ok(mut conn) => {
                    let redis_key = self.build_task_result_key(session_id, task_id);
                    let set_result: redis::RedisResult<()> = redis::AsyncCommands::set_ex(
                        &mut *conn,
                        redis_key,
                        result_location,
                        REDIS_TASK_RESULT_TTL_SECONDS,
                    )
                    .await;
                    if let Err(e) = set_result {
                        log::warn!(
                            "failed to persist task result location in redis for task {}: {}",
                            task_id,
                            e
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "failed to get redis connection from pool for task {}: {}",
                        task_id,
                        e
                    );
                }
            }
        }
    }

    pub async fn get_task_result_location(
        &self,
        session_id: &str,
        task_id: &str,
    ) -> Option<String> {
        let session_id = session_id.trim();
        let task_id = task_id.trim();
        if session_id.is_empty() || task_id.is_empty() {
            return None;
        }

        let cache_key = format!("{}:{}", session_id, task_id);
        let guard = self.task_result_locations.read().await;
        if let Some(location) = guard.get(&cache_key).cloned() {
            return Some(location);
        }
        drop(guard);

        let pool = self.ensure_redis_pool().await?;
        let mut conn = pool.get().await.ok()?;
        let redis_key = self.build_task_result_key(session_id, task_id);

        let redis_value: redis::RedisResult<Option<String>> =
            redis::AsyncCommands::get(&mut *conn, redis_key).await;

        match redis_value {
            Ok(Some(location)) => {
                let mut write_guard = self.task_result_locations.write().await;
                write_guard.insert(cache_key, location.clone());
                Some(location)
            }
            Ok(None) => None,
            Err(e) => {
                log::warn!(
                    "failed to read task result location from redis for task {}: {}",
                    task_id,
                    e
                );
                None
            }
        }
    }
}
