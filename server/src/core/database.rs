use crate::core::DomainResource;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use crate::services::metastore_client::metastore_service as ms;
use crate::services::metastore_client::MetastoreClient;

#[derive(Clone, Debug)]
pub struct KionasDatabase {
    pub id: String,
    pub name: String,
    pub owner: String,
}

impl KionasDatabase {
    pub fn new(name: impl Into<String>, owner: impl Into<String>) -> Self {
        Self { id: uuid::Uuid::new_v4().to_string(), name: name.into(), owner: owner.into() }
    }

    pub async fn apply(self, session_id: &str, shared_data: &SharedData) -> Result<String, String> {
        // Dispatch task to worker
        match helpers::run_task_for_input(shared_data, session_id, "create_database", self.name.clone(), 30).await {
            Ok(loc) => {
                // Persist to metastore as schema for now
                let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateSchema(ms::CreateSchemaRequest { schema_name: self.name })) };
                match MetastoreClient::connect_with_shared(shared_data).await {
                    Ok(mut client) => match client.execute(mreq).await {
                        Ok(_) => Ok(format!("Database created successfully: {}", loc)),
                        Err(e) => Err(format!("Metastore Execute failed for CreateDatabase: {}", e)),
                    },
                    Err(e) => Err(format!("Failed to connect to metastore for CreateDatabase: {}", e)),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

impl DomainResource for KionasDatabase {
    fn kind(&self) -> &'static str { "database" }

    fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            Err("database name cannot be empty".to_string())
        } else {
            Ok(())
        }
    }
}
