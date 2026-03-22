use kionas::get_digest;
use kionas::session::SessionProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const SESSION_KEY_PREFIX: &str = "kionas:session";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    auth_token: String,
    is_authenticated: bool,
    role: String,
    warehouse: String,
    remote_addr: String,
    last_active: u64,
    use_database: String,
}

impl Session {
    pub fn new(
        id: String,
        auth_token: String,
        role: String,
        warehouse: String,
        remote_addr: String,
    ) -> Self {
        Self {
            id,
            auth_token,
            is_authenticated: false,
            role,
            warehouse,
            remote_addr,
            last_active: 0,
            use_database: "default".to_string(),
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
        self.warehouse.clone()
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
        get_digest(&self.warehouse)
    }

    pub fn set_warehouse(&mut self, warehouse: String) {
        self.warehouse = warehouse;
    }

    pub fn get_use_database(&self) -> String {
        self.use_database.clone()
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
        self.provider.set(&key, &value).await.unwrap();
    }

    pub async fn get_session(&self, id: String) -> Option<Session> {
        let key = Self::session_key(&id);
        let value = self.provider.get(&key).await.unwrap();
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
    pub async fn update_session(&self, id: String, session: &mut Session) {
        let key = Self::session_key(&id);
        let value = serde_json::to_string(&session).unwrap();
        self.provider.set(&key, &value).await.unwrap();
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
