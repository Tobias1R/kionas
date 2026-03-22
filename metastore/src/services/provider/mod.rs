pub(crate) mod postgres;

use crate::services::metastore_service::metastore_service::{
    CreateSchemaRequest, CreateSchemaResponse, CreateTableRequest, CreateTableResponse,
    DropSchemaRequest, DropSchemaResponse, DropTableRequest, DropTableResponse, GetSchemaRequest,
    GetSchemaResponse, GetTableRequest, GetTableResponse, GetTableStatisticsRequest,
    GetTableStatisticsResponse, UpdateTableStatisticsRequest, UpdateTableStatisticsResponse,
};
use async_trait::async_trait;

#[allow(dead_code)]
#[async_trait]
pub trait MetastoreProvider: Send + Sync {
    async fn create_table(&self, req: CreateTableRequest) -> CreateTableResponse;
    async fn drop_table(&self, req: DropTableRequest) -> DropTableResponse;
    async fn get_table(&self, req: GetTableRequest) -> GetTableResponse;
    async fn create_schema(&self, req: CreateSchemaRequest) -> CreateSchemaResponse;
    async fn drop_schema(&self, req: DropSchemaRequest) -> DropSchemaResponse;
    async fn get_schema(&self, req: GetSchemaRequest) -> GetSchemaResponse;
    async fn update_table_statistics(
        &self,
        req: UpdateTableStatisticsRequest,
    ) -> UpdateTableStatisticsResponse;
    async fn get_table_statistics(
        &self,
        req: GetTableStatisticsRequest,
    ) -> GetTableStatisticsResponse;
}
