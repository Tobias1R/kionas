use kionas::constants::{REDIS_SESSION_TTL_ENV, REDIS_SESSION_TTL_SECONDS};
use kionas::get_digest;
use kionas::session::SessionProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const SESSION_KEY_PREFIX: &str = "kionas:session";

#[cfg(test)]
#[path = "tests/session_observability_tests.rs"]
mod tests;

/// What: Get the configured session TTL from environment or use default.
///
/// Inputs:
/// - None
///
/// Output:
/// - TTL duration in seconds
///
/// Details:
/// - Checks KIONAS_SESSION_TTL_SECONDS environment variable
/// - Falls back to REDIS_SESSION_TTL_SECONDS constant if not set
/// - Returns u64 for use with Redis SETEX command
fn get_session_ttl() -> u64 {
    std::env::var(REDIS_SESSION_TTL_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(REDIS_SESSION_TTL_SECONDS)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    auth_token: String,
    is_authenticated: bool,
    role: String,
    #[serde(default, alias = "warehouse")]
    warehouse_name: String,
    #[serde(default)]
    pool_members: Vec<String>,
    remote_addr: String,
    last_active: u64,
    use_database: String,
    #[serde(default)]
    query_count: u64,
    #[serde(default)]
    last_query_at: u64,
    #[serde(default)]
    total_query_duration_ms: u64,
    #[serde(default)]
    error_count: u64,
}

impl Session {
    pub fn new(
        id: String,
        auth_token: String,
        role: String,
        warehouse_name: String,
        pool_members: Vec<String>,
        remote_addr: String,
    ) -> Self {
        Self {
            id,
            auth_token,
            is_authenticated: false,
            role,
            warehouse_name,
            pool_members,
            remote_addr,
            last_active: 0,
            use_database: "default".to_string(),
            query_count: 0,
            last_query_at: 0,
            total_query_duration_ms: 0,
            error_count: 0,
        }
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_auth_token(&self) -> String {
        self.auth_token.clone()
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_warehouse(&self) -> String {
        self.warehouse_name.clone()
    }

    /// What: Return a snapshot of pool members captured for this session.
    ///
    /// Inputs:
    /// - None
    ///
    /// Output:
    /// - Vector of worker names bound to this session at creation/update time
    ///
    /// Details:
    /// - Snapshot remains stable even if active pool membership changes later
    #[allow(dead_code)]
    pub fn get_pool_members(&self) -> Vec<String> {
        self.pool_members.clone()
    }

    /// What: Check whether the session is assigned to a specific pool.
    ///
    /// Inputs:
    /// - `pool_name`: Pool name to compare with this session assignment
    ///
    /// Output:
    /// - `true` when the session warehouse matches `pool_name`
    ///
    /// Details:
    /// - Comparison is exact and case-sensitive
    #[allow(dead_code)]
    pub fn is_in_pool(&self, pool_name: &str) -> bool {
        self.warehouse_name == pool_name
    }

    pub fn get_remote_addr(&self) -> String {
        self.remote_addr.clone()
    }

    pub fn authenticate(&mut self, auth_token: String) {
        if self.auth_token == auth_token {
            self.is_authenticated = true;
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated
    }

    #[allow(dead_code)]
    pub fn update_last_active(&mut self) {
        self.last_active = chrono::Utc::now().timestamp() as u64;
    }

    pub fn get_last_active(&self) -> u64 {
        self.last_active
    }

    #[allow(dead_code)]
    pub fn get_last_active_timestamp(&self) -> String {
        chrono::DateTime::<chrono::Utc>::from_timestamp(self.last_active as i64, 0)
            .map(|datetime| datetime.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string())
    }

    pub fn get_warehouse_uuid(&self) -> String {
        get_digest(&self.warehouse_name)
    }

    pub fn set_warehouse(&mut self, warehouse: String) {
        self.warehouse_name = warehouse;
    }

    /// What: Replace the session pool member snapshot.
    ///
    /// Inputs:
    /// - `pool_members`: New pool member list to persist in session payload
    ///
    /// Output:
    /// - None
    ///
    /// Details:
    /// - Intended for session creation and explicit pool rebind operations
    pub fn set_pool_members(&mut self, pool_members: Vec<String>) {
        self.pool_members = pool_members;
    }

    pub fn get_use_database(&self) -> String {
        self.use_database.clone()
    }

    /// What: Get cumulative query dispatch count for this session.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Total number of tracked queries for the session.
    ///
    /// Details:
    /// - Increments on both successful and failed query completions.
    pub fn get_query_count(&self) -> u64 {
        self.query_count
    }

    /// What: Get the epoch-millisecond timestamp of the most recent tracked query.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Millisecond UNIX timestamp, or 0 when no query has been tracked.
    ///
    /// Details:
    /// - Updated by `record_query_completion`.
    pub fn get_last_query_at(&self) -> u64 {
        self.last_query_at
    }

    /// What: Get cumulative wall-clock query duration for this session.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Total query duration in milliseconds.
    ///
    /// Details:
    /// - Uses saturating arithmetic to avoid overflow panics.
    pub fn get_total_query_duration_ms(&self) -> u64 {
        self.total_query_duration_ms
    }

    /// What: Get cumulative tracked query errors for this session.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Total failed query count.
    ///
    /// Details:
    /// - Incremented only when `record_query_completion` receives `succeeded = false`.
    pub fn get_error_count(&self) -> u64 {
        self.error_count
    }

    /// What: Record completion metrics for one query handled under this session.
    ///
    /// Inputs:
    /// - `duration_ms`: Wall-clock query duration in milliseconds.
    /// - `succeeded`: Whether query execution succeeded.
    ///
    /// Output:
    /// - Updates in-memory session observability counters.
    ///
    /// Details:
    /// - Query count increments for both success and failure outcomes.
    /// - Error count increments only for failed outcomes.
    /// - Duration accumulation is saturating to avoid overflow panics.
    pub fn record_query_completion(&mut self, duration_ms: u64, succeeded: bool) {
        self.query_count = self.query_count.saturating_add(1);
        self.last_query_at = chrono::Utc::now().timestamp_millis() as u64;
        self.total_query_duration_ms = self.total_query_duration_ms.saturating_add(duration_ms);
        if !succeeded {
            self.error_count = self.error_count.saturating_add(1);
        }
    }

    #[allow(dead_code)]
    pub fn set_use_database(&mut self, database: String) {
        self.use_database = database;
    }
}

#[derive(Clone, Debug)]
pub struct SessionManager {
    provider: SessionProvider,
}

impl SessionManager {
    fn session_key(id: &str) -> String {
        format!("{}:{}", SESSION_KEY_PREFIX, id)
    }

    fn session_key_pattern() -> String {
        format!("{}:*", SESSION_KEY_PREFIX)
    }

    fn extract_id_from_key(key: &str) -> String {
        key.strip_prefix(&format!("{}:", SESSION_KEY_PREFIX))
            .unwrap_or(key)
            .to_string()
    }

    pub fn new() -> Self {
        let redis_url = std::env::var("REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "redis://kionas-redis:6379/0".to_string());

        Self {
            provider: SessionProvider::new(&redis_url),
        }
    }

    pub async fn add_session(&self, session: Session) {
        println!("Add session: {:?}", session);
        let key = Self::session_key(&session.get_id());
        let value = serde_json::to_string(&session).unwrap();
        let ttl = get_session_ttl();
        self.provider.set_ex(&key, &value, ttl).await.unwrap();
    }

    pub async fn get_session(&self, id: String) -> Option<Session> {
        let key = Self::session_key(&id);
        let value = match self.provider.get(&key).await {
            Ok(value) => value,
            Err(error) => {
                log::warn!("failed to get session '{}' from redis: {}", id, error);
                return None;
            }
        };
        serde_json::from_str::<Session>(&value).ok()
    }

    #[allow(dead_code)]
    pub async fn remove_session(&self, id: String) {
        let key = Self::session_key(&id);
        self.provider.del(&key).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn authenticate_session(&self, id: String, auth_token: String) -> bool {
        let mut session = self.get_session(id.clone()).await.unwrap();
        session.authenticate(auth_token);
        self.add_session(session).await;
        true
    }

    // update session
    pub async fn update_session(&self, id: String, session: &Session) -> Result<(), String> {
        let key = Self::session_key(&id);
        let value = serde_json::to_string(session)
            .map_err(|error| format!("failed to serialize session '{}': {}", id, error))?;
        let ttl = get_session_ttl();
        self.provider
            .set_ex(&key, &value, ttl)
            .await
            .map_err(|error| format!("failed to update session '{}' in redis: {}", id, error))
    }

    // get token session
    pub async fn get_token_session(&self, token: String) -> Option<Session> {
        let mut sessions = HashMap::new();
        let key_pattern = Self::session_key_pattern();
        let keys = match self.provider.keys(&key_pattern).await {
            Ok(keys) => keys,
            Err(err) => {
                log::warn!("failed to list session keys from redis: {err}");
                return None;
            }
        };
        for key in keys {
            let value = match self.provider.get(&key).await {
                Ok(value) => value,
                Err(err) => {
                    log::warn!("failed to load session key '{key}' from redis: {err}");
                    continue;
                }
            };
            match serde_json::from_str::<Session>(&value) {
                Ok(session) => {
                    let session = Arc::new(Mutex::new(session));
                    let session_id = Self::extract_id_from_key(&key);
                    sessions.insert(session_id, session);
                }
                Err(err) => {
                    log::warn!("failed to deserialize session payload for key '{key}': {err}");
                }
            }
        }
        for (_key, session) in sessions {
            let session = session.lock().await;
            if session.get_auth_token() == token {
                return Some(session.clone());
            }
        }
        None
    }

    // list sessions
    pub async fn list_sessions(&self) -> Vec<Session> {
        let mut sessions = Vec::new();
        let key_pattern = Self::session_key_pattern();
        let keys = match self.provider.keys(&key_pattern).await {
            Ok(keys) => keys,
            Err(err) => {
                log::warn!("failed to list sessions from redis: {err}");
                return sessions;
            }
        };

        for key in keys {
            let value = match self.provider.get(&key).await {
                Ok(value) => value,
                Err(err) => {
                    log::warn!("failed to read session key '{key}' from redis: {err}");
                    continue;
                }
            };

            match serde_json::from_str::<Session>(&value) {
                Ok(session) => sessions.push(session),
                Err(err) => {
                    log::warn!("failed to deserialize session for key '{key}': {err}");
                }
            }
        }
        sessions
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

// Helper: find a Warehouse by its name in shared state
// Returns a cloned `Warehouse` if found.
pub async fn find_warehouse_by_name(
    shared: &crate::warehouse::state::SharedData,
    name: &str,
) -> Option<crate::warehouse::Warehouse> {
    let sd = shared.lock().await;
    let wh_map = sd.warehouses.lock().await;
    for (_k, wh) in wh_map.iter() {
        if wh.get_name() == name {
            return Some(wh.clone());
        }
    }
    None
}
