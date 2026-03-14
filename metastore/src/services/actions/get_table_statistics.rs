use super::super::metastore_service::{metastore_service, metastore_service::GetTableStatisticsRequest, metastore_service::GetTableStatisticsResponse};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;
use tonic::{Response, Status};

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: GetTableStatisticsRequest) -> Result<Response<GetTableStatisticsResponse>, Status> {
    let resp = provider.get_table_statistics(req).await;
    Ok(Response::new(resp))
}
