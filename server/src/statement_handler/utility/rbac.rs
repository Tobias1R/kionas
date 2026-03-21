use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::warehouse::state::SharedData;

const OUTCOME_PREFIX: &str = "RESULT";

/// What: Formats a structured direct-command outcome payload.
///
/// Inputs:
/// - `category`: Outcome category (SUCCESS, VALIDATION, INFRA)
/// - `code`: Stable machine-readable outcome code
/// - `message`: Human-readable operation detail
///
/// Output:
/// - Structured outcome string consumed by the SQL command path
///
/// Details:
/// - Keeps a stable prefix and field order so client parsing stays deterministic.
fn format_outcome(category: &str, code: &str, message: impl Into<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        OUTCOME_PREFIX,
        category,
        code,
        message.into()
    )
}

/// What: Normalizes SQL identifiers for direct RBAC command parsing.
///
/// Inputs:
/// - `raw`: Raw token extracted from command text
///
/// Output:
/// - Lowercased, de-quoted identifier without trailing semicolon
///
/// Details:
/// - Supports common quote forms used across SQL dialects.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Normalizes role names to canonical storage format.
///
/// Inputs:
/// - `raw`: Raw role token from command text
///
/// Output:
/// - Uppercased normalized role name
///
/// Details:
/// - Roles are stored in uppercase to simplify uniqueness and matching.
fn normalize_role_name(raw: &str) -> String {
    normalize_identifier(raw).to_ascii_uppercase()
}

/// What: Parses a fixed-position final-name token from command tokens.
///
/// Inputs:
/// - `tokens`: Tokenized SQL command
/// - `expected_len`: Required token length for the command variant
///
/// Output:
/// - Parsed normalized identifier when the shape is valid
///
/// Details:
/// - Returns `None` when token count or parsed value is invalid.
fn parse_simple_name(tokens: &[&str], expected_len: usize) -> Option<String> {
    if tokens.len() != expected_len {
        return None;
    }
    let value = normalize_identifier(tokens[expected_len - 1]);
    if value.is_empty() {
        return None;
    }
    Some(value)
}

/// What: Detects whether a query should be handled by RBAC direct-command routing.
///
/// Inputs:
/// - `lower_query`: Trimmed query text lowercased by caller
///
/// Output:
/// - `true` when query starts with a supported RBAC command
///
/// Details:
/// - This route is used as parser fallback for scaffolded RBAC statements.
fn is_rbac_direct_command(lower_query: &str) -> bool {
    lower_query.starts_with("create user ")
        || lower_query.starts_with("delete user ")
        || lower_query.starts_with("create group ")
        || lower_query.starts_with("delete group ")
        || lower_query.starts_with("create role ")
        || lower_query.starts_with("drop role ")
        || lower_query.starts_with("delete role ")
        || lower_query.starts_with("grant role ")
}

/// What: Resolves the RBAC grant actor from current session context.
///
/// Inputs:
/// - `shared_data`: Shared server state with session manager
/// - `session_id`: Active SQL session identifier
///
/// Output:
/// - Actor identifier used in metastore grant records
///
/// Details:
/// - Falls back to `kionas` when no session role metadata is available.
async fn granted_by_from_session(shared_data: &SharedData, session_id: &str) -> String {
    let session_opt = {
        let state = shared_data.lock().await;
        state
            .session_manager
            .get_session(session_id.to_string())
            .await
    };

    session_opt
        .map(|session| session.get_role())
        .filter(|role| !role.trim().is_empty())
        .unwrap_or_else(|| "kionas".to_string())
}

/// What: Handles RBAC management commands through direct SQL text routing.
///
/// Inputs:
/// - `query`: Raw SQL command text
/// - `session_id`: Active SQL session identifier
/// - `shared_data`: Shared server state used to reach metastore and session context
///
/// Output:
/// - `Some(outcome)` when command is RBAC-related; `None` otherwise
///
/// Details:
/// - This intermission path performs routing/orchestration only.
/// - Enforcement is deferred to Phase 6 authorization work.
pub(crate) async fn maybe_handle_rbac_direct_command(
    query: &str,
    session_id: &str,
    shared_data: &SharedData,
) -> Option<String> {
    let q_trim = query.trim();
    let q_lower = q_trim.to_ascii_lowercase();
    if !is_rbac_direct_command(&q_lower) {
        return None;
    }

    let tokens = q_trim.split_whitespace().collect::<Vec<_>>();
    let mut metastore_client = match MetastoreClient::connect_with_shared(shared_data).await {
        Ok(client) => client,
        Err(e) => {
            return Some(format_outcome(
                "INFRA",
                "METASTORE_CONNECT_FAILED",
                format!("failed to connect metastore for rbac command: {}", e),
            ));
        }
    };

    let request = if q_lower.starts_with("create user ") {
        let username = match parse_simple_name(&tokens, 3) {
            Some(name) => name,
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "CREATE USER requires syntax: CREATE USER <username>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::CreateUser(
                ms::CreateUserRequest { username },
            )),
        }
    } else if q_lower.starts_with("delete user ") {
        let username = match parse_simple_name(&tokens, 3) {
            Some(name) => name,
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "DELETE USER requires syntax: DELETE USER <username>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::DeleteUser(
                ms::DeleteUserRequest { username },
            )),
        }
    } else if q_lower.starts_with("create group ") {
        let group_name = match parse_simple_name(&tokens, 3) {
            Some(name) => name,
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "CREATE GROUP requires syntax: CREATE GROUP <group_name>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::CreateGroup(
                ms::CreateGroupRequest { group_name },
            )),
        }
    } else if q_lower.starts_with("delete group ") {
        let group_name = match parse_simple_name(&tokens, 3) {
            Some(name) => name,
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "DELETE GROUP requires syntax: DELETE GROUP <group_name>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::DeleteGroup(
                ms::DeleteGroupRequest { group_name },
            )),
        }
    } else if q_lower.starts_with("create role ") {
        let role_name = match parse_simple_name(&tokens, 3) {
            Some(name) => normalize_role_name(&name),
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "CREATE ROLE requires syntax: CREATE ROLE <role_name>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::CreateRole(
                ms::CreateRoleRequest {
                    role_name,
                    description: String::new(),
                },
            )),
        }
    } else if q_lower.starts_with("drop role ") || q_lower.starts_with("delete role ") {
        let role_name = match parse_simple_name(&tokens, 3) {
            Some(name) => normalize_role_name(&name),
            None => {
                return Some(format_outcome(
                    "VALIDATION",
                    "INVALID_RBAC_COMMAND",
                    "DROP ROLE/DELETE ROLE requires syntax: DROP ROLE <role_name>",
                ));
            }
        };
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::DropRole(
                ms::DropRoleRequest { role_name },
            )),
        }
    } else if q_lower.starts_with("grant role ") {
        // Syntax: GRANT ROLE <role_name> TO USER <username>
        // Syntax: GRANT ROLE <role_name> TO GROUP <group_name>
        if tokens.len() != 6 || !tokens[3].eq_ignore_ascii_case("to") {
            return Some(format_outcome(
                "VALIDATION",
                "INVALID_RBAC_COMMAND",
                "GRANT ROLE requires syntax: GRANT ROLE <role_name> TO USER <username> or GRANT ROLE <role_name> TO GROUP <group_name>",
            ));
        }

        let role_name = normalize_role_name(tokens[2]);
        let principal_type = normalize_identifier(tokens[4]);
        if principal_type != "user" && principal_type != "group" {
            return Some(format_outcome(
                "VALIDATION",
                "INVALID_RBAC_COMMAND",
                "GRANT ROLE principal type must be USER or GROUP",
            ));
        }
        let principal_name = normalize_identifier(tokens[5]);
        if role_name.is_empty() || principal_name.is_empty() {
            return Some(format_outcome(
                "VALIDATION",
                "INVALID_RBAC_COMMAND",
                "GRANT ROLE role_name and principal_name cannot be empty",
            ));
        }

        let granted_by = granted_by_from_session(shared_data, session_id).await;
        ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::GrantRole(
                ms::GrantRoleRequest {
                    role_name,
                    principal_type,
                    principal_name,
                    granted_by,
                },
            )),
        }
    } else {
        return Some(format_outcome(
            "VALIDATION",
            "INVALID_RBAC_COMMAND",
            "unsupported RBAC direct command",
        ));
    };

    let result = match metastore_client.execute(request).await {
        Ok(resp) => resp.result,
        Err(e) => {
            return Some(format_outcome(
                "INFRA",
                "METASTORE_RBAC_EXEC_FAILED",
                format!("rbac command execution failed: {}", e),
            ));
        }
    };

    let response = match result {
        Some(ms::metastore_response::Result::CreateUserResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::DeleteUserResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::CreateGroupResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::DeleteGroupResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::CreateRoleResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::DropRoleResponse(r)) => (r.success, r.message),
        Some(ms::metastore_response::Result::GrantRoleResponse(r)) => (r.success, r.message),
        _ => {
            return Some(format_outcome(
                "INFRA",
                "METASTORE_PROTOCOL_ERROR",
                "metastore returned unexpected response for RBAC command",
            ));
        }
    };

    if response.0 {
        Some(format_outcome(
            "SUCCESS",
            "RBAC_COMMAND_APPLIED",
            response.1,
        ))
    } else {
        Some(format_outcome(
            "VALIDATION",
            "RBAC_COMMAND_REJECTED",
            response.1,
        ))
    }
}
