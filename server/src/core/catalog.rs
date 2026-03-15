use crate::core::DomainResource;

#[derive(Clone, Debug)]
pub struct KionasCatalog {
    pub id: String,
    pub name: String,
    pub owner: String,
}

impl KionasCatalog {
    pub fn new(name: impl Into<String>, owner: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            owner: owner.into(),
        }
    }
}

impl DomainResource for KionasCatalog {
    fn kind(&self) -> &'static str { "catalog" }

    fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            Err("catalog name cannot be empty".to_string())
        } else {
            Ok(())
        }
    }
}
