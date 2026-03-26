use crate::parser::datafusion_sql::sqlparser::ast::{
    CatalogSyncNamespaceMode, ContactEntry, ObjectName, StorageSerializationPolicy, Tag,
};
use crate::providers::normalize_identifier;
use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::statement_handler::shared::helpers;
use crate::warehouse::state::SharedData;

const OUTCOME_PREFIX: &str = "RESULT";

/// What: A borrowed view of the CREATE DATABASE AST fields used by the server orchestrator.
///
/// Inputs:
/// - `db_name`: Parsed SQL object name for the target database.
/// - Other fields: Full CREATE DATABASE options captured from the AST.
///
/// Output:
/// - A structured view used to build worker payloads and validate behavior.
///
/// Details:
/// - The server currently uses only a subset for behavior (`db_name`, `if_not_exists`) but
///   persists all fields into the worker payload for forward compatibility.
pub(crate) struct CreateDatabaseAst<'a> {
    pub(crate) db_name: &'a ObjectName,
    pub(crate) if_not_exists: bool,
    pub(crate) location: Option<&'a String>,
    pub(crate) managed_location: Option<&'a String>,
    pub(crate) or_replace: bool,
    pub(crate) transient: bool,
    pub(crate) clone: Option<&'a ObjectName>,
    pub(crate) data_retention_time_in_days: Option<u64>,
    pub(crate) max_data_extension_time_in_days: Option<u64>,
    pub(crate) external_volume: Option<&'a String>,
    pub(crate) catalog: Option<&'a String>,
    pub(crate) replace_invalid_characters: Option<bool>,
    pub(crate) default_ddl_collation: Option<&'a String>,
    pub(crate) storage_serialization_policy: Option<&'a StorageSerializationPolicy>,
    pub(crate) comment: Option<&'a String>,
    pub(crate) catalog_sync: Option<&'a String>,
    pub(crate) catalog_sync_namespace_mode: Option<&'a CatalogSyncNamespaceMode>,
    pub(crate) catalog_sync_namespace_flatten_delimiter: Option<&'a String>,
    pub(crate) with_tags: Option<&'a Vec<Tag>>,
    pub(crate) with_contacts: Option<&'a Vec<ContactEntry>>,
}

/// What: Encode an orchestrator outcome into a structured string result.
///
/// Inputs:
/// - `category`: Outcome category (`SUCCESS`, `VALIDATION`, `INFRA`).
/// - `code`: Stable machine-readable code.
/// - `message`: User-facing message text.
///
/// Output:
/// - Encoded outcome string consumed by API response mapping.
///
/// Details:
/// - The format is `RESULT|<category>|<code>|<message>`.
fn format_outcome(category: &str, code: &str, message: impl Into<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        OUTCOME_PREFIX,
        category,
        code,
        message.into()
    )
}

/// What: Resolve canonical database name from SQL `ObjectName`.
///
/// Inputs:
/// - `db_name`: Parsed SQL object name.
///
/// Output:
/// - Canonical lowercase database name.
///
/// Details:
/// - For multipart names, the rightmost part is treated as the database name.
fn normalize_database_name(db_name: &ObjectName) -> Result<String, String> {
    let rendered = db_name.to_string();
    let last_part = rendered
        .rsplit('.')
        .next()
        .map(normalize_identifier)
        .unwrap_or_default();

    if last_part.trim().is_empty() {
        return Err("database name cannot be empty".to_string());
    }

    Ok(last_part)
}

/// What: Validate server-side database naming rules for CREATE DATABASE.
///
/// Inputs:
/// - `name`: Canonical database name.
///
/// Output:
/// - `true` when the name is valid.
///
/// Details:
/// - Accepted characters are `[a-zA-Z0-9_-]`.
fn is_valid_database_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Determine whether metastore error message denotes missing database.
///
/// Inputs:
/// - `msg`: Raw metastore message.
///
/// Output:
/// - `true` when message maps to not-found semantics.
///
/// Details:
/// - This keeps transport errors separate from expected lookup misses.
fn is_not_found_message(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("not found")
}

/// What: Determine whether metastore error message denotes duplicate database.
///
/// Inputs:
/// - `msg`: Raw metastore message.
///
/// Output:
/// - `true` when message maps to uniqueness violation semantics.
///
/// Details:
/// - Used to handle check-then-create races deterministically.
fn is_duplicate_message(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("duplicate") || lower.contains("already exists") || lower.contains("unique")
}

/// What: Build a deterministic worker payload that preserves the full CREATE DATABASE AST shape.
///
/// Inputs:
/// - `ast`: Parsed CREATE DATABASE fields.
/// - `normalized_name`: Canonical database name used by server/worker for deterministic paths.
///
/// Output:
/// - JSON payload serialized as a string.
///
/// Details:
/// - The payload stores all known AST options as string/primitive values so future worker logic
///   can use additional fields without changing task payload structure.
fn build_worker_payload(ast: &CreateDatabaseAst<'_>, normalized_name: &str) -> String {
    let payload = serde_json::json!({
        "statement": "CreateDatabase",
        "db_name": {
            "raw": ast.db_name.to_string(),
            "normalized": normalized_name,
        },
        "if_not_exists": ast.if_not_exists,
        "location": ast.location,
        "managed_location": ast.managed_location,
        "or_replace": ast.or_replace,
        "transient": ast.transient,
        "clone": ast.clone.map(std::string::ToString::to_string),
        "data_retention_time_in_days": ast.data_retention_time_in_days,
        "max_data_extension_time_in_days": ast.max_data_extension_time_in_days,
        "external_volume": ast.external_volume,
        "catalog": ast.catalog,
        "replace_invalid_characters": ast.replace_invalid_characters,
        "default_ddl_collation": ast.default_ddl_collation,
        "storage_serialization_policy": ast.storage_serialization_policy.map(|v| format!("{:?}", v)),
        "comment": ast.comment,
        "catalog_sync": ast.catalog_sync,
        "catalog_sync_namespace_mode": ast.catalog_sync_namespace_mode.map(|v| format!("{:?}", v)),
        "catalog_sync_namespace_flatten_delimiter": ast.catalog_sync_namespace_flatten_delimiter,
        "with_tags": ast.with_tags.map(|tags| tags.iter().map(std::string::ToString::to_string).collect::<Vec<_>>()),
        "with_contacts": ast.with_contacts.map(|entries| entries.iter().map(std::string::ToString::to_string).collect::<Vec<_>>()),
    });

    payload.to_string()
}

/// What: Orchestrate server-owned CREATE DATABASE flow across metastore pre-check, worker task,
/// and metastore persistence.
///
/// Inputs:
/// - `shared_data`: Shared server state for worker and metastore connectivity.
/// - `session_id`: Current session identifier used for worker routing and correlation logs.
/// - `ast`: Borrowed CREATE DATABASE AST fields.
///
/// Output:
/// - Encoded outcome string in the format `RESULT|<category>|<code>|<message>`.
///
/// Details:
/// - Validation and existence checks are performed in metastore first.
/// - Worker receives typed operation payload, not SQL.
/// - Metastore create is executed only after worker success.
pub(crate) async fn handle_create_database(
    shared_data: &SharedData,
    session_id: &str,
    ast: CreateDatabaseAst<'_>,
) -> String {
    let normalized_name = match normalize_database_name(ast.db_name) {
        Ok(name) => name,
        Err(e) => {
            return format_outcome("VALIDATION", "INVALID_DATABASE_NAME", e);
        }
    };

    if !is_valid_database_name(&normalized_name) {
        return format_outcome(
            "VALIDATION",
            "INVALID_DATABASE_NAME",
            format!(
                "invalid database name '{}': only [a-zA-Z0-9_-] are allowed",
                normalized_name
            ),
        );
    }

    let mut metastore_client = match MetastoreClient::connect_with_shared(shared_data).await {
        Ok(client) => client,
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_CONNECT_FAILED",
                format!("failed to connect to metastore: {}", e),
            );
        }
    };

    let get_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetDatabase(
            ms::GetDatabaseRequest {
                database_name: normalized_name.clone(),
            },
        )),
    };

    let database_exists = match metastore_client.execute(get_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetDatabaseResponse(get_db_resp)) => {
                if get_db_resp.success {
                    true
                } else if is_not_found_message(&get_db_resp.message) {
                    false
                } else {
                    return format_outcome(
                        "INFRA",
                        "METASTORE_GET_DATABASE_FAILED",
                        format!(
                            "failed to validate database existence in metastore: {}",
                            get_db_resp.message
                        ),
                    );
                }
            }
            _ => {
                return format_outcome(
                    "INFRA",
                    "METASTORE_PROTOCOL_ERROR",
                    "metastore returned an unexpected response for get_database",
                );
            }
        },
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_GET_DATABASE_FAILED",
                format!("failed to validate database existence in metastore: {}", e),
            );
        }
    };

    if database_exists {
        if ast.if_not_exists {
            return format_outcome(
                "SUCCESS",
                "DATABASE_ALREADY_EXISTS_SKIPPED",
                format!(
                    "database already exists: {} (skipped because IF NOT EXISTS was set)",
                    normalized_name
                ),
            );
        }

        return format_outcome(
            "VALIDATION",
            "DATABASE_ALREADY_EXISTS",
            format!("database already exists: {}", normalized_name),
        );
    }

    let worker_payload = build_worker_payload(&ast, &normalized_name);
    let worker_result_location = match helpers::run_task_for_input(
        shared_data,
        session_id,
        "create_database",
        worker_payload,
        60,
    )
    .await
    {
        Ok(location) => location,
        Err(e) => {
            return format_outcome(
                "INFRA",
                "WORKER_CREATE_DATABASE_FAILED",
                format!("worker failed to initialize database storage: {}", e),
            );
        }
    };

    let create_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::CreateDatabase(
            ms::CreateDatabaseRequest {
                database_name: normalized_name.clone(),
            },
        )),
    };

    match metastore_client.execute(create_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::CreateDatabaseResponse(create_db_resp)) => {
                if create_db_resp.success {
                    log::info!(
                        "create_database success: session_id={} database={} worker_location={}",
                        session_id,
                        normalized_name,
                        worker_result_location
                    );
                    format_outcome(
                        "SUCCESS",
                        "DATABASE_CREATED",
                        format!(
                            "database created successfully: {} (location: {})",
                            normalized_name, worker_result_location
                        ),
                    )
                } else if is_duplicate_message(&create_db_resp.message) {
                    format_outcome(
                        "VALIDATION",
                        "DATABASE_ALREADY_EXISTS",
                        format!("database already exists: {}", normalized_name),
                    )
                } else {
                    log::error!(
                        "create_database partial failure: session_id={} database={} worker_location={} metastore_error={}",
                        session_id,
                        normalized_name,
                        worker_result_location,
                        create_db_resp.message
                    );
                    format_outcome(
                        "INFRA",
                        "METASTORE_CREATE_AFTER_WORKER_FAILED",
                        format!(
                            "database storage initialized but metastore write failed: {}",
                            create_db_resp.message
                        ),
                    )
                }
            }
            _ => format_outcome(
                "INFRA",
                "METASTORE_PROTOCOL_ERROR",
                "metastore returned an unexpected response for create_database",
            ),
        },
        Err(e) => {
            log::error!(
                "create_database partial failure: session_id={} database={} worker_location={} metastore_error={}",
                session_id,
                normalized_name,
                worker_result_location,
                e
            );
            format_outcome(
                "INFRA",
                "METASTORE_CREATE_AFTER_WORKER_FAILED",
                format!(
                    "database storage initialized but metastore write failed: {}",
                    e
                ),
            )
        }
    }
}
