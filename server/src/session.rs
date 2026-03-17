use kionas::get_digest;
use kionas::session::SessionProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    pub fn update_last_active(&mut self) {
        self.last_active = chrono::Utc::now().timestamp() as u64;
    }

    pub fn get_last_active(&self) -> u64 {
        self.last_active
    }

    pub fn get_last_active_timestamp(&self) -> String {
        let datetime = chrono::NaiveDateTime::from_timestamp(self.last_active as i64, 0);
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
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

    pub fn set_use_database(&mut self, database: String) {
        self.use_database = database;
    }
}

#[derive(Clone, Debug)]
pub struct SessionManager {
    provider: SessionProvider,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            provider: SessionProvider::new("redis://redis:6379/0"),
        }
    }

    pub async fn add_session(&self, session: Session) {
        println!("Add session: {:?}", session);
        let key = session.get_id();
        let value = serde_json::to_string(&session).unwrap();
        self.provider.set(&key, &value).await.unwrap();
    }

    pub async fn get_session(&self, id: String) -> Option<Session> {
        let value = self.provider.get(&id).await.unwrap();
        match serde_json::from_str::<Session>(&value) {
            Ok(session) => Some(session),
            Err(_) => None,
        }
    }

    pub async fn remove_session(&self, id: String) {
        self.provider.del(&id).await.unwrap();
    }

    pub async fn authenticate_session(&self, id: String, auth_token: String) -> bool {
        let mut session = self.get_session(id.clone()).await.unwrap();
        session.authenticate(auth_token);
        self.add_session(session).await;
        true
    }

    // update session
    pub async fn update_session(&self, id: String, session: &mut Session) {
        let key = id;
        let value = serde_json::to_string(&session).unwrap();
        self.provider.set(&key, &value).await.unwrap();
    }

    // get token session
    pub async fn get_token_session(&self, token: String) -> Option<Session> {
        let mut sessions = HashMap::new();
        let keys = self.provider.keys("*").await.unwrap();
        for key in keys {
            let value = self.provider.get(&key).await.unwrap();
            match serde_json::from_str::<Session>(&value) {
                Ok(session) => {
                    let session = Arc::new(Mutex::new(session));
                    sessions.insert(key, session);
                }
                Err(_) => {}
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
        let keys = self.provider.keys("*").await.unwrap();
        for key in keys {
            let value = self.provider.get(&key).await.unwrap();
            match serde_json::from_str::<Session>(&value) {
                Ok(session) => sessions.push(session),
                Err(_) => {}
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
