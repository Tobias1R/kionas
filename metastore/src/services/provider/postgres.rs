use crate::services::metastore_service::metastore_service::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateSchemaRequest, CreateSchemaResponse,
    CreateTableRequest, CreateTableResponse, DropSchemaRequest, DropSchemaResponse,
    DropTableRequest, DropTableResponse, GetDatabaseRequest, GetDatabaseResponse, GetSchemaRequest,
    GetSchemaResponse, GetTableRequest, GetTableResponse, GetTableStatisticsRequest,
    GetTableStatisticsResponse, UpdateTableStatisticsRequest, UpdateTableStatisticsResponse,
};
use crate::services::metastore_service::metastore_service::{
    CreateTransactionRequest, CreateTransactionResponse, GetTransactionRequest,
    GetTransactionResponse, UpdateTransactionStateRequest, UpdateTransactionStateResponse,
};
use async_trait::async_trait;
use kionas::config::PostgresServiceConfig;
use uuid::Uuid;

use deadpool_postgres::Pool as PostgresPool;
use log;
use tokio_postgres::NoTls;

#[async_trait]
pub trait MetastoreProvider: Send + Sync {
    async fn create_table(&self, req: CreateTableRequest) -> CreateTableResponse;
    async fn drop_table(&self, req: DropTableRequest) -> DropTableResponse;
    async fn get_table(&self, req: GetTableRequest) -> GetTableResponse;
    async fn create_schema(&self, req: CreateSchemaRequest) -> CreateSchemaResponse;
    async fn drop_schema(&self, req: DropSchemaRequest) -> DropSchemaResponse;
    async fn get_schema(&self, req: GetSchemaRequest) -> GetSchemaResponse;
    async fn create_database(&self, req: CreateDatabaseRequest) -> CreateDatabaseResponse;
    async fn get_database(&self, req: GetDatabaseRequest) -> GetDatabaseResponse;
    async fn update_table_statistics(
        &self,
        req: UpdateTableStatisticsRequest,
    ) -> UpdateTableStatisticsResponse;
    async fn get_table_statistics(
        &self,
        req: GetTableStatisticsRequest,
    ) -> GetTableStatisticsResponse;
    // Transaction operations
    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse;
    async fn update_transaction_state(
        &self,
        req: UpdateTransactionStateRequest,
    ) -> UpdateTransactionStateResponse;
    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse;
}

// Example Postgres provider stub
pub struct PostgresProvider {
    pub pool: PostgresPool,
}

impl PostgresProvider {
    pub fn new(
        config: &PostgresServiceConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut pg_cfg = tokio_postgres::Config::new();
        pg_cfg.host(&config.postgres_host);
        pg_cfg.port(config.postgres_port);
        pg_cfg.dbname(&config.postgres_db);
        pg_cfg.user(&config.postgres_user);
        pg_cfg.password(&config.postgres_password);
        let mgr = deadpool_postgres::Manager::new(pg_cfg, NoTls);
        let pool = deadpool_postgres::Pool::builder(mgr).max_size(16).build()?;
        Ok(PostgresProvider { pool })
    }
}

#[async_trait]
impl MetastoreProvider for PostgresProvider {
    async fn create_table(&self, _req: CreateTableRequest) -> CreateTableResponse {
        // TODO: Implement Postgres logic
        CreateTableResponse {
            success: true,
            message: "Table created".to_string(),
        }
    }

    async fn create_schema(&self, req: CreateSchemaRequest) -> CreateSchemaResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateSchemaResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };
        let stmt = "INSERT INTO catalogs (name, owner) VALUES ($1, $2) RETURNING id";
        let owner = "kionas";
        // print
        log::debug!(
            "Creating schema '{}' with owner '{}'",
            req.schema_name,
            owner
        );
        match client.query_one(stmt, &[&req.schema_name, &owner]).await {
            Ok(_row) => CreateSchemaResponse {
                success: true,
                message: format!("Schema '{}' created", req.schema_name),
            },
            Err(e) => CreateSchemaResponse {
                success: false,
                message: format!("Failed to create schema: {}", e),
            },
        }
    }

    async fn drop_table(&self, _req: DropTableRequest) -> DropTableResponse {
        DropTableResponse {
            success: true,
            message: "Table dropped".to_string(),
        }
    }
    async fn get_table(&self, _req: GetTableRequest) -> GetTableResponse {
        GetTableResponse {
            success: true,
            message: "Table fetched".to_string(),
            metadata: None,
        }
    }
    async fn drop_schema(&self, _req: DropSchemaRequest) -> DropSchemaResponse {
        DropSchemaResponse {
            success: true,
            message: "Schema dropped".to_string(),
        }
    }
    async fn get_schema(&self, _req: GetSchemaRequest) -> GetSchemaResponse {
        GetSchemaResponse {
            success: true,
            message: "Schema fetched".to_string(),
            metadata: None,
        }
    }

    async fn create_database(&self, req: CreateDatabaseRequest) -> CreateDatabaseResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateDatabaseResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let stmt = "INSERT INTO catalogs (name, owner) VALUES ($1, $2) RETURNING id";
        let owner = "kionas";
        match client.query_one(stmt, &[&req.database_name, &owner]).await {
            Ok(_row) => CreateDatabaseResponse {
                success: true,
                message: format!("Database '{}' created", req.database_name),
            },
            Err(e) => CreateDatabaseResponse {
                success: false,
                message: format!("Failed to create database: {}", e),
            },
        }
    }

    async fn get_database(&self, req: GetDatabaseRequest) -> GetDatabaseResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetDatabaseResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    metadata: None,
                };
            }
        };

        let stmt = "SELECT id, name FROM catalogs WHERE name = $1 LIMIT 1";
        match client.query_opt(stmt, &[&req.database_name]).await {
            Ok(Some(row)) => {
                let id: i64 = row.get(0);
                let name: String = row.get(1);
                GetDatabaseResponse {
                    success: true,
                    message: format!("Database '{}' found", name),
                    metadata: Some(
                        crate::services::metastore_service::metastore_service::DatabaseMetadata {
                            uuid: id.to_string(),
                            database_name: name,
                            location: String::new(),
                            container: String::new(),
                        },
                    ),
                }
            }
            Ok(None) => GetDatabaseResponse {
                success: false,
                message: "database not found".to_string(),
                metadata: None,
            },
            Err(e) => GetDatabaseResponse {
                success: false,
                message: format!("Failed to get database: {}", e),
                metadata: None,
            },
        }
    }

    async fn update_table_statistics(
        &self,
        _req: UpdateTableStatisticsRequest,
    ) -> UpdateTableStatisticsResponse {
        UpdateTableStatisticsResponse {
            success: true,
            message: "Statistics updated".to_string(),
        }
    }
    async fn get_table_statistics(
        &self,
        _req: GetTableStatisticsRequest,
    ) -> GetTableStatisticsResponse {
        GetTableStatisticsResponse {
            success: true,
            message: "Statistics fetched".to_string(),
            statistics: None,
        }
    }

    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateTransactionResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
            }
        };

        // Build participants JSON
        let mut parts = Vec::new();
        for p in req.participants.iter() {
            let obj = serde_json::json!({"id": p.id, "target": p.target, "staging_prefix": p.staging_prefix});
            parts.push(obj);
        }
        let parts_json = serde_json::Value::Array(parts);
        let parts_str = parts_json.to_string();

        let tx_id_str = &req.tx_id;
        // Use parameterized query to avoid SQL injection and driver ToSql issues
        // Cast parameter 1 as text then to uuid so we can pass tx_id as &str
        // Cast $3 as text then to jsonb so we can pass participants as a string
        let stmt = "INSERT INTO transactions (tx_id, state, participants) VALUES ($1::text::uuid, $2, $3::text::jsonb) RETURNING tx_id::text";
        log::debug!(
            "[metastore::postgres] Executing parameterized SQL: {}",
            stmt
        );
        match client
            .query_one(stmt, &[&tx_id_str, &"PREPARING", &parts_str])
            .await
        {
            Ok(row) => {
                let returned: Result<&str, _> = row.try_get(0);
                match returned {
                    Ok(s) => match Uuid::parse_str(s) {
                        Ok(id) => log::debug!("[metastore::postgres] Insert returned tx_id={}", id),
                        Err(pe) => log::debug!(
                            "[metastore::postgres] Insert returned tx_id string but failed to parse UUID: {} (raw='{}')",
                            pe,
                            s
                        ),
                    },
                    Err(e) => log::debug!(
                        "[metastore::postgres] Insert returned a row (unable to extract tx_id): {}",
                        e
                    ),
                }
                CreateTransactionResponse {
                    success: true,
                    message: format!("Transaction {} created", req.tx_id),
                    transaction: None,
                }
            }
            Err(e) => {
                log::debug!("[metastore::postgres] Failed to create transaction: {}", e);
                CreateTransactionResponse {
                    success: false,
                    message: format!("Failed to create transaction: {}", e),
                    transaction: None,
                }
            }
        }
    }

    async fn update_transaction_state(
        &self,
        req: UpdateTransactionStateRequest,
    ) -> UpdateTransactionStateResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return UpdateTransactionStateResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
            }
        };
        let tx_id_str = &req.tx_id;
        let stmt = format!(
            "UPDATE transactions SET state = $1, updated_at = CURRENT_TIMESTAMP WHERE tx_id = '{}'::uuid RETURNING tx_id",
            tx_id_str
        );
        match client
            .query_one(&stmt, &[&format!("{:?}", req.state)])
            .await
        {
            Ok(_row) => UpdateTransactionStateResponse {
                success: true,
                message: format!("Transaction {} updated", req.tx_id),
                transaction: None,
            },
            Err(e) => UpdateTransactionStateResponse {
                success: false,
                message: format!("Failed to update transaction: {}", e),
                transaction: None,
            },
        }
    }

    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetTransactionResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
            }
        };
        let stmt = "SELECT tx_id, state, participants, created_at, updated_at FROM transactions WHERE tx_id = $1";
        match client.query_one(stmt, &[&req.tx_id]).await {
            Ok(_row) => {
                // Optionally parse participants JSON, but return minimal response
                GetTransactionResponse {
                    success: true,
                    message: "Found".to_string(),
                    transaction: None,
                }
            }
            Err(e) => GetTransactionResponse {
                success: false,
                message: format!("Failed to get transaction: {}", e),
                transaction: None,
            },
        }
    }
}
