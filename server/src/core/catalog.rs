use crate::core::DomainResource;
use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;

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
    fn kind(&self) -> &'static str {
        "catalog"
    }

    fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            Err("catalog name cannot be empty".to_string())
        } else {
            Ok(())
        }
    }
}

impl KionasCatalog {
    pub async fn apply(self, session_id: &str, shared_data: &SharedData) -> Result<String, String> {
        // Dispatch task to worker
        match helpers::run_task_for_input(
            shared_data,
            session_id,
            "create_schema",
            self.name.clone(),
            30,
        )
        .await
        {
            Ok(loc) => {
                // Persist to metastore
                let mreq = ms::MetastoreRequest {
                    action: Some(ms::metastore_request::Action::CreateSchema(
                        ms::CreateSchemaRequest {
                            schema_name: self.name,
                            database_name: String::new(),
                            ast_payload_json: String::new(),
                        },
                    )),
                };
                match MetastoreClient::connect_with_shared(shared_data).await {
                    Ok(mut client) => match client.execute(mreq).await {
                        Ok(_) => Ok(format!("Schema created successfully: {}", loc)),
                        Err(e) => Err(format!("Metastore Execute failed for CreateSchema: {}", e)),
                    },
                    Err(e) => Err(format!(
                        "Failed to connect to metastore for CreateSchema: {}",
                        e
                    )),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}
