//pub mod warehouse_service_server;
pub(crate) mod state;

use core::hash;
use std::sync::Arc;
use tokio::sync::Mutex;
// yaml
#[derive(Debug, Clone)]
pub struct Warehouse {
    name: String,
    uuid: String,
    host: String,
    port: u32,
    warehouse_type: String,
    last_heartbeat: chrono::DateTime<chrono::Utc>,
}

impl Warehouse {
    pub fn new(
        name: String,
        uuid: String,
        host: String,
        port: u32,
        warehouse_type: String,
    ) -> Self {
        Self {
            name,
            uuid,
            host,
            port,
            warehouse_type,
            last_heartbeat: chrono::Utc::now(),
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_uuid(&self) -> String {
        self.uuid.clone()
    }

    pub fn get_host(&self) -> String {
        self.host.clone()
    }

    pub fn get_port(&self) -> u32 {
        self.port
    }

    #[allow(dead_code)]
    pub fn get_warehouse_type(&self) -> String {
        self.warehouse_type.clone()
    }

    #[allow(dead_code)]
    pub fn get_connection_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn set_last_heartbeat(&mut self, last_heartbeat: chrono::DateTime<chrono::Utc>) {
        self.last_heartbeat = last_heartbeat;
    }
}

// implement trait hash for Warehouse
impl std::hash::Hash for Warehouse {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.host.hash(state);
        self.port.hash(state);
        self.warehouse_type.hash(state);
    }
}

#[allow(dead_code)]
pub struct WarehouseManager {
    warehouses: Arc<Mutex<Vec<Warehouse>>>,
}

#[allow(dead_code)]
impl WarehouseManager {
    pub fn new() -> Self {
        Self {
            warehouses: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_warehouse(&self, warehouse: Warehouse) {
        let mut warehouses = self.warehouses.lock().await;
        warehouses.push(warehouse);
    }

    pub async fn get_warehouses(&self) -> Vec<Warehouse> {
        let warehouses = self.warehouses.lock().await;
        warehouses.clone()
    }

    pub async fn get_warehouse(&self, uuid: &str) -> Option<Warehouse> {
        let warehouses = self.warehouses.lock().await;
        for warehouse in warehouses.iter() {
            if warehouse.get_uuid() == uuid {
                return Some(warehouse.clone());
            }
        }
        None
    }
}
