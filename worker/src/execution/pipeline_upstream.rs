use crate::execution::planner::StageExecutionContext;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightDescriptor;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use tonic14::Request;
use tonic14::metadata::MetadataValue;
use tonic14::transport::Endpoint;

/// What: Normalize configured upstream Flight endpoint to an HTTP URI.
///
/// Inputs:
/// - `raw`: Endpoint string from stage context metadata.
///
/// Output:
/// - Normalized endpoint URI suitable for tonic transport setup.
///
/// Details:
/// - Preserves explicit `http://` or `https://` schemes.
/// - Defaults to `http://` when no scheme is provided.
fn normalize_flight_endpoint(raw: &str) -> Result<String, String> {
    let endpoint = raw.trim();
    if endpoint.is_empty() {
        return Err("upstream flight endpoint is empty".to_string());
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    Ok(format!("http://{}", endpoint))
}

/// What: Select upstream partitions consumed by a downstream partition.
///
/// Inputs:
/// - `upstream_partition_count`: Total upstream partition count.
/// - `downstream_partition_count`: Total downstream partition count.
/// - `downstream_partition_index`: Current downstream partition index.
///
/// Output:
/// - Deterministic list of upstream partition ids to pull.
///
/// Details:
/// - Implements one-to-one and many-to-one distribution mapping.
fn selected_upstream_partitions_for_downstream(
    upstream_partition_count: u32,
    downstream_partition_count: u32,
    downstream_partition_index: u32,
) -> Vec<u32> {
    if downstream_partition_count <= 1 {
        return (0..upstream_partition_count).collect::<Vec<_>>();
    }

    if upstream_partition_count <= downstream_partition_count {
        if downstream_partition_index < upstream_partition_count {
            return vec![downstream_partition_index];
        }
        return Vec::new();
    }

    (0..upstream_partition_count)
        .filter(|partition_id| {
            partition_id % downstream_partition_count == downstream_partition_index
        })
        .collect::<Vec<_>>()
}

/// What: Attach pull-dispatch metadata used for upstream Flight auth and routing.
///
/// Inputs:
/// - `request`: Mutable tonic request to decorate.
/// - `session_id`: Query session identifier.
/// - `stage_context`: Stage execution context with auth/rbac metadata.
///
/// Output:
/// - `Ok(())` when all required metadata is attached.
///
/// Details:
/// - Returns actionable errors when required metadata is missing or invalid.
fn insert_pull_dispatch_metadata(
    request: &mut Request<impl Sized>,
    session_id: &str,
    stage_context: &StageExecutionContext,
) -> Result<(), String> {
    let rbac_user = stage_context.rbac_user.as_deref().ok_or_else(|| {
        "missing __rbac_user in stage context for upstream Flight pull".to_string()
    })?;
    let rbac_role = stage_context.rbac_role.as_deref().ok_or_else(|| {
        "missing __rbac_role in stage context for upstream Flight pull".to_string()
    })?;
    let auth_scope = stage_context.auth_scope.as_deref().ok_or_else(|| {
        "missing __auth_scope in stage context for upstream Flight pull".to_string()
    })?;
    let query_id = stage_context.query_id.as_deref().ok_or_else(|| {
        "missing __query_id in stage context for upstream Flight pull".to_string()
    })?;

    let metadata = request.metadata_mut();
    metadata.insert(
        "session_id",
        MetadataValue::try_from(session_id)
            .map_err(|e| format!("invalid session_id metadata: {}", e))?,
    );
    metadata.insert(
        "rbac_user",
        MetadataValue::try_from(rbac_user)
            .map_err(|e| format!("invalid rbac_user metadata: {}", e))?,
    );
    metadata.insert(
        "rbac_role",
        MetadataValue::try_from(rbac_role)
            .map_err(|e| format!("invalid rbac_role metadata: {}", e))?,
    );
    metadata.insert(
        "auth_scope",
        MetadataValue::try_from(auth_scope)
            .map_err(|e| format!("invalid auth_scope metadata: {}", e))?,
    );
    metadata.insert(
        "query_id",
        MetadataValue::try_from(query_id)
            .map_err(|e| format!("invalid query_id metadata: {}", e))?,
    );

    Ok(())
}

/// What: Pull and decode upstream input batches through Flight endpoints.
///
/// Inputs:
/// - `session_id`: Query session identifier.
/// - `stage_context`: Stage context containing upstream endpoint maps and partition metadata.
///
/// Output:
/// - Collected upstream `RecordBatch` values for this downstream partition.
///
/// Details:
/// - Pulls selected upstream partitions, requests tickets, then decodes each stream frame.
/// - Logs an informational message and returns an empty vector when upstream is empty.
pub(crate) async fn load_upstream_flight_batches(
    session_id: &str,
    stage_context: &StageExecutionContext,
) -> Result<Vec<RecordBatch>, String> {
    let mut all_batches = Vec::<RecordBatch>::new();

    for upstream_stage_id in &stage_context.upstream_stage_ids {
        let endpoint_by_partition = stage_context
            .upstream_stage_flight_endpoints
            .get(upstream_stage_id)
            .ok_or_else(|| {
                format!(
                    "missing upstream Flight endpoint map for upstream stage {}",
                    upstream_stage_id
                )
            })?;

        let upstream_partition_count = stage_context
            .upstream_partition_counts
            .get(upstream_stage_id)
            .copied()
            .unwrap_or(1);
        let selected_partitions = selected_upstream_partitions_for_downstream(
            upstream_partition_count,
            stage_context.partition_count,
            stage_context.partition_index,
        );

        for upstream_partition_id in selected_partitions {
            let endpoint_raw = endpoint_by_partition
                .get(&upstream_partition_id)
                .or_else(|| endpoint_by_partition.get(&0))
                .ok_or_else(|| {
                    format!(
                        "missing upstream Flight endpoint for stage {} partition {}",
                        upstream_stage_id, upstream_partition_id
                    )
                })?;
            let endpoint = normalize_flight_endpoint(endpoint_raw)?;
            let channel = Endpoint::from_shared(endpoint.clone())
                .map_err(|e| format!("invalid upstream Flight endpoint '{}': {}", endpoint, e))?
                .connect()
                .await
                .map_err(|e| {
                    format!(
                        "failed to connect upstream Flight endpoint '{}': {}",
                        endpoint, e
                    )
                })?;
            let max_message_bytes = 16 * 1024 * 1024usize;
            let mut client = FlightServiceClient::new(channel)
                .max_decoding_message_size(max_message_bytes)
                .max_encoding_message_size(max_message_bytes);

            let mut info_request = Request::new(FlightDescriptor {
                path: vec![
                    session_id.to_string(),
                    upstream_stage_id.to_string(),
                    upstream_partition_id.to_string(),
                ],
                cmd: Bytes::from_static(b"STAGE_STREAM"),
                ..Default::default()
            });
            insert_pull_dispatch_metadata(&mut info_request, session_id, stage_context)?;

            let flight_info = client
                .get_flight_info(info_request)
                .await
                .map_err(|e| {
                    format!(
                        "get_flight_info failed for upstream stage {} partition {} at '{}': {}",
                        upstream_stage_id, upstream_partition_id, endpoint, e
                    )
                })?
                .into_inner();

            for flight_endpoint in flight_info.endpoint {
                let ticket = flight_endpoint.ticket.ok_or_else(|| {
                    format!(
                        "missing Flight ticket for upstream stage {} partition {}",
                        upstream_stage_id, upstream_partition_id
                    )
                })?;
                let mut get_request = Request::new(ticket);
                insert_pull_dispatch_metadata(&mut get_request, session_id, stage_context)?;

                let response_stream = client
                    .do_get(get_request)
                    .await
                    .map_err(|e| {
                        format!(
                            "do_get failed for upstream stage {} partition {} at '{}': {}",
                            upstream_stage_id, upstream_partition_id, endpoint, e
                        )
                    })?
                    .into_inner();

                let flight_data_stream =
                    stream::try_unfold(response_stream, |mut upstream| async {
                        match upstream.message().await {
                            Ok(Some(frame)) => Ok(Some((frame, upstream))),
                            Ok(None) => Ok(None),
                            Err(e) => Err(tonic14::Status::internal(format!(
                                "failed to read upstream Flight frame: {}",
                                e
                            ))),
                        }
                    })
                    .map_err(Into::into);

                let mut decoder = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);
                while let Some(batch) = decoder.next().await {
                    let batch = batch.map_err(|e| {
                        format!(
                            "failed to decode upstream Flight batch for stage {} partition {}: {}",
                            upstream_stage_id, upstream_partition_id, e
                        )
                    })?;
                    all_batches.push(batch);
                }
            }
        }
    }

    if all_batches.is_empty() {
        log::info!(
            "upstream Flight pulls for stage {} returned no batches; continuing with empty input",
            stage_context.stage_id
        );
    } else {
        log::info!(
            "loaded upstream Flight input for stage_id={} upstream={:?} batches= {}",
            stage_context.stage_id,
            stage_context.upstream_stage_ids,
            all_batches.len()
        );
    }

    Ok(all_batches)
}
