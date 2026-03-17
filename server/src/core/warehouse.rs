use crate::core::DomainResource;

#[derive(Clone, Debug)]
pub struct KionasWarehouse {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub ca_cert: Option<String>,
}

impl KionasWarehouse {
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            host: host.into(),
            port,
            ca_cert: None,
        }
    }
}

impl DomainResource for KionasWarehouse {
    fn kind(&self) -> &'static str {
        "warehouse"
    }

    fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("warehouse name cannot be empty".to_string());
        }
        if self.host.trim().is_empty() {
            return Err("warehouse host cannot be empty".to_string());
        }
        Ok(())
    }
}
