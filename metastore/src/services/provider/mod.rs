
pub(crate) mod postgres;

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
