use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{Expr, ObjectName, SchemaName, SqlOption};

const OUTCOME_PREFIX: &str = "RESULT";

/// What: A borrowed view of the CREATE SCHEMA AST fields used by the server orchestrator.
///
/// Inputs:
/// - `schema_name`: Parsed SQL schema target.
/// - Other fields: Full CREATE SCHEMA options captured from the AST.
///
/// Output:
/// - A structured view used to build worker payloads and validate behavior.
///
/// Details:
/// - The server currently uses a subset for behavior but keeps all fields in the worker payload
///   and metastore write for forward compatibility.
pub(crate) struct CreateSchemaAst<'a> {
    pub(crate) schema_name: &'a SchemaName,
    pub(crate) if_not_exists: bool,
    pub(crate) with: Option<&'a Vec<SqlOption>>,
    pub(crate) options: Option<&'a Vec<SqlOption>>,
    pub(crate) default_collate_spec: Option<&'a Expr>,
    pub(crate) clone: Option<&'a ObjectName>,
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

/// What: Normalize SQL identifier fragments for deterministic comparisons and paths.
///
/// Inputs:
/// - `raw`: Identifier text.
///
/// Output:
/// - Lowercase normalized identifier without common quoting characters.
///
/// Details:
/// - Supports common quoting styles: `"name"`, `` `name` ``, and `[name]`.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Validate server-side naming rules for CREATE SCHEMA namespace parts.
///
/// Inputs:
/// - `name`: Canonical database or schema name.
///
/// Output:
/// - `true` when the name is valid.
///
/// Details:
/// - Accepted characters are `[a-zA-Z0-9_-]`.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Determine whether metastore error message denotes missing resource.
///
/// Inputs:
/// - `msg`: Raw metastore message.
///
/// Output:
/// - `true` when message maps to not-found semantics.
fn is_not_found_message(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("not found")
}

/// What: Determine whether metastore error message denotes uniqueness conflict.
///
/// Inputs:
/// - `msg`: Raw metastore message.
///
/// Output:
/// - `true` when message maps to duplicate semantics.
fn is_duplicate_message(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("duplicate") || lower.contains("already exists") || lower.contains("unique")
}

/// What: Extract canonical database and schema names from a CREATE SCHEMA target.
///
/// Inputs:
/// - `schema_name`: Parsed SQL schema target.
///
/// Output:
/// - Tuple `(database_name, schema_name)` in canonical lowercase form.
///
/// Details:
/// - Uses the rightmost two qualified name parts as `database.schema`.
/// - `AUTHORIZATION`-only targets are rejected in this first implementation.
fn extract_namespace(schema_name: &SchemaName) -> Result<(String, String), String> {
    let object_name = match schema_name {
        SchemaName::Simple(name) | SchemaName::NamedAuthorization(name, _) => name,
        SchemaName::UnnamedAuthorization(_) => {
            return Err(
                "CREATE SCHEMA requires an explicit namespace in the form <database>.<schema>"
                    .to_string(),
            );
        }
    };

    let rendered = object_name.to_string();
    let parts = rendered
        .split('.')
        .map(normalize_identifier)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();

    if parts.len() < 2 {
        return Err(
            "CREATE SCHEMA requires a qualified namespace in the form <database>.<schema>"
                .to_string(),
        );
    }

    let database_name = parts[parts.len() - 2].clone();
    let schema_name = parts[parts.len() - 1].clone();
    Ok((database_name, schema_name))
}

/// What: Build a deterministic worker payload that preserves full CREATE SCHEMA AST shape.
///
/// Inputs:
/// - `ast`: Parsed CREATE SCHEMA fields.
/// - `database_name`: Canonical database name.
/// - `schema_name`: Canonical schema name.
///
/// Output:
/// - JSON payload serialized as a string.
fn build_worker_payload(
    ast: &CreateSchemaAst<'_>,
    database_name: &str,
    schema_name: &str,
) -> String {
    let payload = serde_json::json!({
        "statement": "CreateSchema",
        "namespace": {
            "raw": ast.schema_name.to_string(),
            "database": database_name,
            "schema": schema_name,
        },
        "if_not_exists": ast.if_not_exists,
        "with": ast.with.map(|opts| opts.iter().map(std::string::ToString::to_string).collect::<Vec<_>>()),
        "options": ast.options.map(|opts| opts.iter().map(std::string::ToString::to_string).collect::<Vec<_>>()),
        "default_collate_spec": ast.default_collate_spec.map(std::string::ToString::to_string),
        "clone": ast.clone.map(std::string::ToString::to_string),
        "schema_name_variant": match ast.schema_name {
            SchemaName::Simple(_) => "Simple",
            SchemaName::UnnamedAuthorization(_) => "UnnamedAuthorization",
            SchemaName::NamedAuthorization(_, _) => "NamedAuthorization",
        },
    });

    payload.to_string()
}

/// What: Orchestrate server-owned CREATE SCHEMA flow across metastore pre-check, worker task,
/// and metastore persistence.
///
/// Inputs:
/// - `shared_data`: Shared server state for worker and metastore connectivity.
/// - `session_id`: Current session identifier used for routing and correlation logs.
/// - `ast`: Borrowed CREATE SCHEMA AST fields.
///
/// Output:
/// - Encoded outcome string in the format `RESULT|<category>|<code>|<message>`.
pub(crate) async fn handle_create_schema(
    shared_data: &SharedData,
    session_id: &str,
    ast: CreateSchemaAst<'_>,
) -> String {
    let (database_name, schema_name) = match extract_namespace(ast.schema_name) {
        Ok(parts) => parts,
        Err(e) => return format_outcome("VALIDATION", "INVALID_SCHEMA_NAMESPACE", e),
    };

    if !is_valid_namespace_name(&database_name) {
        return format_outcome(
            "VALIDATION",
            "INVALID_DATABASE_NAME",
            format!(
                "invalid database name '{}': only [a-zA-Z0-9_-] are allowed",
                database_name
            ),
        );
    }
    if !is_valid_namespace_name(&schema_name) {
        return format_outcome(
            "VALIDATION",
            "INVALID_SCHEMA_NAME",
            format!(
                "invalid schema name '{}': only [a-zA-Z0-9_-] are allowed",
                schema_name
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

    let get_db_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetDatabase(
            ms::GetDatabaseRequest {
                database_name: database_name.clone(),
            },
        )),
    };

    match metastore_client.execute(get_db_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetDatabaseResponse(get_db_resp)) => {
                if !get_db_resp.success {
                    if is_not_found_message(&get_db_resp.message) {
                        return format_outcome(
                            "VALIDATION",
                            "DATABASE_NOT_FOUND",
                            format!("database not found: {}", database_name),
                        );
                    }

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
    }

    let get_schema_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetSchema(
            ms::GetSchemaRequest {
                schema_name: schema_name.clone(),
                database_name: database_name.clone(),
            },
        )),
    };

    let schema_exists = match metastore_client.execute(get_schema_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetSchemaResponse(get_schema_resp)) => {
                if get_schema_resp.success {
                    true
                } else if is_not_found_message(&get_schema_resp.message) {
                    false
                } else {
                    return format_outcome(
                        "INFRA",
                        "METASTORE_GET_SCHEMA_FAILED",
                        format!(
                            "failed to validate schema existence in metastore: {}",
                            get_schema_resp.message
                        ),
                    );
                }
            }
            _ => {
                return format_outcome(
                    "INFRA",
                    "METASTORE_PROTOCOL_ERROR",
                    "metastore returned an unexpected response for get_schema",
                );
            }
        },
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_GET_SCHEMA_FAILED",
                format!("failed to validate schema existence in metastore: {}", e),
            );
        }
    };

    if schema_exists {
        if ast.if_not_exists {
            return format_outcome(
                "SUCCESS",
                "SCHEMA_ALREADY_EXISTS_SKIPPED",
                format!(
                    "schema already exists: {}.{} (skipped because IF NOT EXISTS was set)",
                    database_name, schema_name
                ),
            );
        }
        return format_outcome(
            "VALIDATION",
            "SCHEMA_ALREADY_EXISTS",
            format!("schema already exists: {}.{}", database_name, schema_name),
        );
    }

    let worker_payload = build_worker_payload(&ast, &database_name, &schema_name);
    let worker_result_location = match helpers::run_task_for_input(
        shared_data,
        session_id,
        "create_schema",
        worker_payload.clone(),
        60,
    )
    .await
    {
        Ok(location) => location,
        Err(e) => {
            return format_outcome(
                "INFRA",
                "WORKER_CREATE_SCHEMA_FAILED",
                format!("worker failed to initialize schema storage: {}", e),
            );
        }
    };

    let create_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::CreateSchema(
            ms::CreateSchemaRequest {
                schema_name: schema_name.clone(),
                database_name: database_name.clone(),
                ast_payload_json: worker_payload,
            },
        )),
    };

    match metastore_client.execute(create_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::CreateSchemaResponse(create_schema_resp)) => {
                if create_schema_resp.success {
                    log::info!(
                        "create_schema success: session_id={} database={} schema={} worker_location={}",
                        session_id,
                        database_name,
                        schema_name,
                        worker_result_location
                    );
                    format_outcome(
                        "SUCCESS",
                        "SCHEMA_CREATED",
                        format!(
                            "schema created successfully: {}.{} (location: {})",
                            database_name, schema_name, worker_result_location
                        ),
                    )
                } else if is_duplicate_message(&create_schema_resp.message) {
                    format_outcome(
                        "VALIDATION",
                        "SCHEMA_ALREADY_EXISTS",
                        format!("schema already exists: {}.{}", database_name, schema_name),
                    )
                } else {
                    log::error!(
                        "create_schema partial failure: session_id={} database={} schema={} worker_location={} metastore_error={}",
                        session_id,
                        database_name,
                        schema_name,
                        worker_result_location,
                        create_schema_resp.message
                    );
                    format_outcome(
                        "INFRA",
                        "METASTORE_CREATE_AFTER_WORKER_FAILED",
                        format!(
                            "schema storage initialized but metastore write failed: {}",
                            create_schema_resp.message
                        ),
                    )
                }
            }
            _ => format_outcome(
                "INFRA",
                "METASTORE_PROTOCOL_ERROR",
                "metastore returned an unexpected response for create_schema",
            ),
        },
        Err(e) => {
            log::error!(
                "create_schema partial failure: session_id={} database={} schema={} worker_location={} metastore_error={}",
                session_id,
                database_name,
                schema_name,
                worker_result_location,
                e
            );
            format_outcome(
                "INFRA",
                "METASTORE_CREATE_AFTER_WORKER_FAILED",
                format!(
                    "schema storage initialized but metastore write failed: {}",
                    e
                ),
            )
        }
    }
}
