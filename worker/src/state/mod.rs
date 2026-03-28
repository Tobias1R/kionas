use crate::interops::manager::InteropsManager;
use crate::storage::StorageProvider;
use crate::storage::object_store_pool::ObjectStoreManager;
use arrow::record_batch::RecordBatch;
use deadpool::managed::Pool;
use kionas::constants::{
    REDIS_DB_STATUS, REDIS_POOL_SIZE_ENV, REDIS_TASK_RESULT_KEY_PREFIX,
    REDIS_TASK_RESULT_TTL_SECONDS, REDIS_URL_ENV,
};
use kionas::consul::ClusterInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

type RedisPool = Arc<Pool<RedisConnectionManager>>;

pub(crate) struct RedisConnectionManager {
    redis_url: String,
}

impl deadpool::managed::Manager for RedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let redis_url = self.redis_url.clone();
        async move {
            let client = redis::Client::open(redis_url)?;
            let conn = client.get_multiplexed_async_connection().await?;
            Ok(conn)
        }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        Ok(())
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
    pub stage_streaming_state: StageStreamingState,
    #[serde(skip)]
    pub(crate) redis_pool: Arc<tokio::sync::Mutex<Option<RedisPool>>>,
}

type StageStreamKey = (String, u32, u32);
const STAGE_STREAM_TTL_SECONDS_ENV: &str = "WORKER_STAGE_STREAM_TTL_SECONDS";
const STAGE_STREAM_TTL_SECONDS_DEFAULT: u64 = 600;
const STAGE_STREAM_MAX_ENTRIES_ENV: &str = "WORKER_STAGE_STREAM_MAX_ENTRIES";
const STAGE_STREAM_MAX_ENTRIES_DEFAULT: usize = 2048;

#[derive(Clone)]
struct StageStreamEntry {
    stored_at: Instant,
    batches: Vec<RecordBatch>,
}

#[derive(Clone, Default)]
pub struct StageStreamingState {
    batches_by_partition: Arc<tokio::sync::RwLock<HashMap<StageStreamKey, StageStreamEntry>>>,
}

impl StageStreamingState {
    /// What: Resolve TTL used for in-memory stage-stream entries.
    ///
    /// Inputs:
    /// - Reads optional env `WORKER_STAGE_STREAM_TTL_SECONDS`.
    ///
    /// Output:
    /// - Positive TTL duration for stage-stream cache entries.
    ///
    /// Details:
    /// - Falls back to 600 seconds if env is missing, invalid, or zero.
    fn stage_stream_ttl() -> Duration {
        let ttl_seconds = std::env::var(STAGE_STREAM_TTL_SECONDS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(STAGE_STREAM_TTL_SECONDS_DEFAULT);
        Duration::from_secs(ttl_seconds)
    }

    /// What: Resolve maximum retained stage-stream partition entries.
    ///
    /// Inputs:
    /// - Reads optional env `WORKER_STAGE_STREAM_MAX_ENTRIES`.
    ///
    /// Output:
    /// - Positive maximum count of `(session, stage, partition)` entries.
    ///
    /// Details:
    /// - Falls back to 2048 when env is missing, invalid, or zero.
    fn stage_stream_max_entries() -> usize {
        std::env::var(STAGE_STREAM_MAX_ENTRIES_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(STAGE_STREAM_MAX_ENTRIES_DEFAULT)
    }

    /// What: Evict expired stage-stream entries in-place.
    ///
    /// Inputs:
    /// - `guard`: Mutable cache map under write lock.
    /// - `now`: Current monotonic timestamp.
    /// - `ttl`: Entry lifetime window.
    ///
    /// Output:
    /// - Removes stale entries and keeps still-valid partitions.
    fn evict_stale_locked(
        guard: &mut HashMap<StageStreamKey, StageStreamEntry>,
        now: Instant,
        ttl: Duration,
    ) {
        guard.retain(|_, entry| {
            now.checked_duration_since(entry.stored_at)
                .unwrap_or_default()
                <= ttl
        });
    }

    /// What: Enforce stage-stream entry capacity using oldest-first eviction.
    ///
    /// Inputs:
    /// - `guard`: Mutable cache map under write lock.
    /// - `max_entries`: Maximum allowed entry count.
    ///
    /// Output:
    /// - Removes oldest entries until `len <= max_entries`.
    ///
    /// Details:
    /// - When capacity is exceeded, eviction selects the smallest `stored_at` timestamp.
    fn evict_over_capacity_locked(
        guard: &mut HashMap<StageStreamKey, StageStreamEntry>,
        max_entries: usize,
    ) {
        if max_entries == 0 {
            guard.clear();
            return;
        }

        while guard.len() > max_entries {
            let oldest_key = guard
                .iter()
                .min_by_key(|(_, entry)| entry.stored_at)
                .map(|(key, _)| key.clone());
            if let Some(key) = oldest_key {
                let _ = guard.remove(&key);
            } else {
                break;
            }
        }
    }

    /// What: Store stage output batches for one session/stage/partition for downstream pull.
    ///
    /// Inputs:
    /// - `session_id`: Query session identifier.
    /// - `stage_id`: Upstream stage identifier.
    /// - `partition_id`: Upstream partition identifier.
    /// - `batches`: Materialized output batches for the partition.
    ///
    /// Output:
    /// - Replaces any previously stored batches for the same key.
    pub async fn store_partition_batches(
        &self,
        session_id: &str,
        stage_id: u32,
        partition_id: u32,
        batches: Vec<RecordBatch>,
    ) {
        let now = Instant::now();
        let ttl = Self::stage_stream_ttl();
        let max_entries = Self::stage_stream_max_entries();
        let mut guard = self.batches_by_partition.write().await;
        Self::evict_stale_locked(&mut guard, now, ttl);
        guard.insert(
            (session_id.to_string(), stage_id, partition_id),
            StageStreamEntry {
                stored_at: now,
                batches,
            },
        );
        Self::evict_over_capacity_locked(&mut guard, max_entries);
    }

    /// What: Load stage output batches for one session/stage/partition.
    ///
    /// Inputs:
    /// - `session_id`: Query session identifier.
    /// - `stage_id`: Upstream stage identifier.
    /// - `partition_id`: Upstream partition identifier.
    ///
    /// Output:
    /// - `Some(Vec<RecordBatch>)` when batches exist.
    /// - `None` when no stage stream batches are registered.
    pub async fn get_partition_batches(
        &self,
        session_id: &str,
        stage_id: u32,
        partition_id: u32,
    ) -> Option<Vec<RecordBatch>> {
        let key = (session_id.to_string(), stage_id, partition_id);
        let ttl = Self::stage_stream_ttl();
        let now = Instant::now();

        let guard = self.batches_by_partition.read().await;
        if let Some(entry) = guard.get(&key) {
            let is_expired = now
                .checked_duration_since(entry.stored_at)
                .unwrap_or_default()
                > ttl;
            if !is_expired {
                return Some(entry.batches.clone());
            }
        } else {
            return None;
        }
        drop(guard);

        let mut write_guard = self.batches_by_partition.write().await;
        Self::evict_stale_locked(&mut write_guard, now, ttl);
        None
    }

    /// What: Clear all stage stream buffers for one session.
    ///
    /// Inputs:
    /// - `session_id`: Query session identifier.
    ///
    /// Output:
    /// - Removes all stage/partition entries for the session.
    #[allow(dead_code)]
    pub async fn cleanup_session(&self, session_id: &str) {
        let mut guard = self.batches_by_partition.write().await;
        guard.retain(|(sid, _, _), _| sid != session_id);
    }
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
            stage_streaming_state: StageStreamingState::default(),
            redis_pool: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn set_storage_provider(&mut self, prov: Arc<dyn StorageProvider + Send + Sync>) {
        self.storage_provider = Some(prov);
    }

    #[allow(dead_code)]
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

    /// What: Remove all cached task result locations for one session from in-memory cache.
    ///
    /// Inputs:
    /// - `session_id`: Session identifier whose task result locations should be removed.
    ///
    /// Output:
    /// - Removes all in-memory task location entries keyed under the session.
    fn cleanup_task_result_locations_for_session_cache_locked(
        guard: &mut HashMap<String, String>,
        session_id: &str,
    ) {
        guard.retain(|cache_key, _| {
            cache_key
                .split_once(':')
                .map(|(sid, _)| sid != session_id)
                .unwrap_or(true)
        });
    }

    /// What: Remove all cached runtime state for one session.
    ///
    /// Inputs:
    /// - `session_id`: Session identifier to cleanup.
    ///
    /// Output:
    /// - Clears stage-stream partition buffers and task result location cache entries.
    ///
    /// Details:
    /// - Redis cleanup is best-effort and degrades gracefully if Redis is unavailable.
    pub async fn cleanup_runtime_session(&self, session_id: &str) {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return;
        }

        self.stage_streaming_state.cleanup_session(session_id).await;

        {
            let mut guard = self.task_result_locations.write().await;
            Self::cleanup_task_result_locations_for_session_cache_locked(&mut guard, session_id);
        }

        if let Some(pool) = self.ensure_redis_pool().await {
            match pool.get().await {
                Ok(mut conn) => {
                    let key_pattern = format!(
                        "{}:{}:{}:*",
                        REDIS_TASK_RESULT_KEY_PREFIX, self.worker_info.worker_id, session_id
                    );
                    let keys: redis::RedisResult<Vec<String>> =
                        redis::AsyncCommands::keys(&mut *conn, key_pattern.clone()).await;
                    match keys {
                        Ok(redis_keys) => {
                            if !redis_keys.is_empty() {
                                let del_result: redis::RedisResult<usize> =
                                    redis::AsyncCommands::del(&mut *conn, redis_keys).await;
                                if let Err(e) = del_result {
                                    log::warn!(
                                        "failed to delete runtime session keys from redis for session {}: {}",
                                        session_id,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "failed to list runtime session keys from redis for session {} pattern {}: {}",
                                session_id,
                                key_pattern,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "failed to acquire redis connection for runtime session cleanup {}: {}",
                        session_id,
                        e
                    );
                }
            }
        }
    }
}
