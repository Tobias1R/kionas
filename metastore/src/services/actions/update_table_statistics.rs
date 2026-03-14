use super::super::metastore_service::{metastore_service, metastore_service::UpdateTableStatisticsRequest, metastore_service::UpdateTableStatisticsResponse};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;
use tonic::{Response, Status};

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: UpdateTableStatisticsRequest) -> Result<Response<UpdateTableStatisticsResponse>, Status> {
    let resp = provider.update_table_statistics(req).await;
    Ok(Response::new(resp))
}
