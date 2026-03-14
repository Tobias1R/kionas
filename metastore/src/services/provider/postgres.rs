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
use crate::config::MetastoreConfig;

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
}

// Example Postgres provider stub
pub struct PostgresProvider {
    pub pool: PostgresPool,
}

impl PostgresProvider {
    pub fn new(config: &MetastoreConfig) -> Result<Self, Box<dyn std::error::Error>> {
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
}
