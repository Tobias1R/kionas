use async_trait::async_trait;
use crate::services::metastore_service::metastore_service::{
    CreateTableRequest, DropTableRequest, GetTableRequest,
    CreateSchemaRequest, DropSchemaRequest, GetSchemaRequest,
    UpdateTableStatisticsRequest, GetTableStatisticsRequest,
    CreateTableResponse, DropTableResponse, GetTableResponse,
    CreateSchemaResponse, DropSchemaResponse, GetSchemaResponse,
    UpdateTableStatisticsResponse, GetTableStatisticsResponse,
    TableStatistics, TableMetadata, SchemaMetadata
};
use crate::services::metastore_service::metastore_service::{
    CreateTransactionRequest, CreateTransactionResponse,
    UpdateTransactionStateRequest, UpdateTransactionStateResponse,
    GetTransactionRequest, GetTransactionResponse,
};
use kionas::config::PostgresServiceConfig;
use uuid::Uuid;

use deadpool_postgres::{Manager, Pool as PostgresPool};
use tokio_postgres::NoTls;

#[async_trait]
pub trait MetastoreProvider: Send + Sync {
    async fn create_table(&self, req: CreateTableRequest) -> CreateTableResponse;
    async fn drop_table(&self, req: DropTableRequest) -> DropTableResponse;
    async fn get_table(&self, req: GetTableRequest) -> GetTableResponse;
    async fn create_schema(&self, req: CreateSchemaRequest) -> CreateSchemaResponse;
    async fn drop_schema(&self, req: DropSchemaRequest) -> DropSchemaResponse;
    async fn get_schema(&self, req: GetSchemaRequest) -> GetSchemaResponse;
    async fn update_table_statistics(&self, req: UpdateTableStatisticsRequest) -> UpdateTableStatisticsResponse;
    async fn get_table_statistics(&self, req: GetTableStatisticsRequest) -> GetTableStatisticsResponse;
    // Transaction operations
    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse;
    async fn update_transaction_state(&self, req: UpdateTransactionStateRequest) -> UpdateTransactionStateResponse;
    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse;
}

// Example Postgres provider stub
pub struct PostgresProvider {
    pub pool: PostgresPool,
}

impl PostgresProvider {
    pub fn new(config: &PostgresServiceConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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
        CreateTableResponse { success: true, message: "Table created".to_string() }
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
        println!("Creating schema '{}' with owner '{}'", req.schema_name, owner);
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
        DropTableResponse { success: true, message: "Table dropped".to_string() }
    }
    async fn get_table(&self, _req: GetTableRequest) -> GetTableResponse {
        GetTableResponse { success: true, message: "Table fetched".to_string(), metadata: None }
    }
    async fn drop_schema(&self, _req: DropSchemaRequest) -> DropSchemaResponse {
        DropSchemaResponse { success: true, message: "Schema dropped".to_string() }
    }
    async fn get_schema(&self, _req: GetSchemaRequest) -> GetSchemaResponse {
        GetSchemaResponse { success: true, message: "Schema fetched".to_string(), metadata: None }
    }
    async fn update_table_statistics(&self, _req: UpdateTableStatisticsRequest) -> UpdateTableStatisticsResponse {
        UpdateTableStatisticsResponse { success: true, message: "Statistics updated".to_string() }
    }
    async fn get_table_statistics(&self, _req: GetTableStatisticsRequest) -> GetTableStatisticsResponse {
        GetTableStatisticsResponse { success: true, message: "Statistics fetched".to_string(), statistics: None }
    }

    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateTransactionResponse { success: false, message: format!("Failed to get DB client: {}", e), transaction: None };
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
        // Safely escape single quotes in JSON string for SQL literal embedding
        let participants_escaped = parts_str.replace("'", "''");
        let stmt = format!(
            "INSERT INTO transactions (tx_id, state, participants) VALUES ('{}'::uuid, 'PREPARING', '{}'::jsonb) RETURNING tx_id",
            tx_id_str, participants_escaped
        );
        println!("[metastore::postgres] Executing SQL: {}", stmt);
        match client.query_one(&stmt, &[]).await {
            Ok(row) => {
                let returned: Result<&str, _> = row.try_get(0);
                match returned {
                    Ok(s) => match Uuid::parse_str(s) {
                        Ok(id) => println!("[metastore::postgres] Insert returned tx_id={}", id),
                        Err(pe) => println!("[metastore::postgres] Insert returned tx_id string but failed to parse UUID: {} (raw='{}')", pe, s),
                    },
                    Err(e) => println!("[metastore::postgres] Insert returned a row (unable to extract tx_id): {}", e),
                }
                CreateTransactionResponse { success: true, message: format!("Transaction {} created", req.tx_id), transaction: None }
            }
            Err(e) => {
                println!("[metastore::postgres] Failed to create transaction: {}", e);
                CreateTransactionResponse { success: false, message: format!("Failed to create transaction: {}", e), transaction: None }
            }
        }
    }

    async fn update_transaction_state(&self, req: UpdateTransactionStateRequest) -> UpdateTransactionStateResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return UpdateTransactionStateResponse { success: false, message: format!("Failed to get DB client: {}", e), transaction: None };
            }
        };
        let tx_id_str = &req.tx_id;
        let stmt = format!("UPDATE transactions SET state = $1, updated_at = CURRENT_TIMESTAMP WHERE tx_id = '{}'::uuid RETURNING tx_id", tx_id_str);
        match client.query_one(&stmt, &[&format!("{:?}", req.state)]).await {
            Ok(_row) => UpdateTransactionStateResponse { success: true, message: format!("Transaction {} updated", req.tx_id), transaction: None },
            Err(e) => UpdateTransactionStateResponse { success: false, message: format!("Failed to update transaction: {}", e), transaction: None },
        }
    }

    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetTransactionResponse { success: false, message: format!("Failed to get DB client: {}", e), transaction: None };
            }
        };
        let stmt = "SELECT tx_id, state, participants, created_at, updated_at FROM transactions WHERE tx_id = $1";
        match client.query_one(stmt, &[&req.tx_id]).await {
            Ok(_row) => {
                // Optionally parse participants JSON, but return minimal response
                GetTransactionResponse { success: true, message: "Found".to_string(), transaction: None }
            }
            Err(e) => GetTransactionResponse { success: false, message: format!("Failed to get transaction: {}", e), transaction: None },
        }
    }
}
