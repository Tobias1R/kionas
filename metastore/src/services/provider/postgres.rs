use crate::services::metastore_service::metastore_service::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateGroupRequest, CreateGroupResponse,
    CreateRoleRequest, CreateRoleResponse, CreateSchemaRequest, CreateSchemaResponse,
    CreateTableRequest, CreateTableResponse, CreateUserRequest, CreateUserResponse,
    DeleteGroupRequest, DeleteGroupResponse, DeleteUserRequest, DeleteUserResponse,
    DropRoleRequest, DropRoleResponse, DropSchemaRequest, DropSchemaResponse, DropTableRequest,
    DropTableResponse, GetDatabaseRequest, GetDatabaseResponse, GetSchemaRequest,
    GetSchemaResponse, GetTableRequest, GetTableResponse, GetTableStatisticsRequest,
    GetTableStatisticsResponse, GrantRoleRequest, GrantRoleResponse, UpdateTableStatisticsRequest,
    UpdateTableStatisticsResponse,
};
use crate::services::metastore_service::metastore_service::{
    CreateTransactionRequest, CreateTransactionResponse, GetTransactionRequest,
    GetTransactionResponse, UpdateTransactionStateRequest, UpdateTransactionStateResponse,
};
use async_trait::async_trait;
use kionas::config::PostgresServiceConfig;
use uuid::Uuid;

use deadpool_postgres::Pool as PostgresPool;
use log;
use tokio_postgres::NoTls;

#[async_trait]
pub trait MetastoreProvider: Send + Sync {
    async fn create_table(&self, req: CreateTableRequest) -> CreateTableResponse;
    async fn drop_table(&self, req: DropTableRequest) -> DropTableResponse;
    async fn get_table(&self, req: GetTableRequest) -> GetTableResponse;
    async fn create_schema(&self, req: CreateSchemaRequest) -> CreateSchemaResponse;
    async fn drop_schema(&self, req: DropSchemaRequest) -> DropSchemaResponse;
    async fn get_schema(&self, req: GetSchemaRequest) -> GetSchemaResponse;
    async fn create_database(&self, req: CreateDatabaseRequest) -> CreateDatabaseResponse;
    async fn get_database(&self, req: GetDatabaseRequest) -> GetDatabaseResponse;
    async fn update_table_statistics(
        &self,
        req: UpdateTableStatisticsRequest,
    ) -> UpdateTableStatisticsResponse;
    async fn get_table_statistics(
        &self,
        req: GetTableStatisticsRequest,
    ) -> GetTableStatisticsResponse;
    // Transaction operations
    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse;
    async fn update_transaction_state(
        &self,
        req: UpdateTransactionStateRequest,
    ) -> UpdateTransactionStateResponse;
    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse;
    /// What: Creates a new RBAC user principal.
    ///
    /// Inputs:
    /// - `req`: Create user payload containing `username`
    ///
    /// Output:
    /// - `CreateUserResponse` with success flag and message
    ///
    /// Details:
    /// - Reserved principal validation is applied by provider implementation.
    async fn create_user(&self, req: CreateUserRequest) -> CreateUserResponse;
    /// What: Deletes an existing RBAC user principal.
    ///
    /// Inputs:
    /// - `req`: Delete user payload containing `username`
    ///
    /// Output:
    /// - `DeleteUserResponse` with success flag and message
    ///
    /// Details:
    /// - Reserved principals are protected from deletion.
    async fn delete_user(&self, req: DeleteUserRequest) -> DeleteUserResponse;
    /// What: Creates a new RBAC group principal.
    ///
    /// Inputs:
    /// - `req`: Create group payload containing `group_name`
    ///
    /// Output:
    /// - `CreateGroupResponse` with success flag and message
    ///
    /// Details:
    /// - Group name uniqueness is enforced by database constraints.
    async fn create_group(&self, req: CreateGroupRequest) -> CreateGroupResponse;
    /// What: Deletes an RBAC group principal.
    ///
    /// Inputs:
    /// - `req`: Delete group payload containing `group_name`
    ///
    /// Output:
    /// - `DeleteGroupResponse` with success flag and message
    ///
    /// Details:
    /// - Dependent relationships are handled by schema constraints.
    async fn delete_group(&self, req: DeleteGroupRequest) -> DeleteGroupResponse;
    /// What: Creates a new RBAC role.
    ///
    /// Inputs:
    /// - `req`: Create role payload with name and optional description
    ///
    /// Output:
    /// - `CreateRoleResponse` with success flag and message
    ///
    /// Details:
    /// - Role names are normalized by implementation before persistence.
    async fn create_role(&self, req: CreateRoleRequest) -> CreateRoleResponse;
    /// What: Removes an RBAC role.
    ///
    /// Inputs:
    /// - `req`: Drop role payload containing `role_name`
    ///
    /// Output:
    /// - `DropRoleResponse` with success flag and message
    ///
    /// Details:
    /// - Reserved ADMIN role is protected from removal.
    async fn drop_role(&self, req: DropRoleRequest) -> DropRoleResponse;
    /// What: Binds an RBAC role to a user or group principal.
    ///
    /// Inputs:
    /// - `req`: Grant payload with role, principal type/name, and actor
    ///
    /// Output:
    /// - `GrantRoleResponse` with success flag and message
    ///
    /// Details:
    /// - Provider validates principal and role existence before insert.
    async fn grant_role(&self, req: GrantRoleRequest) -> GrantRoleResponse;
}

// Example Postgres provider stub
pub struct PostgresProvider {
    pub pool: PostgresPool,
}

impl PostgresProvider {
    pub fn new(
        config: &PostgresServiceConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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

/// What: Formats Postgres errors for RBAC operations with actionable diagnostics.
///
/// Inputs:
/// - `context`: Operation-specific prefix for the error message
/// - `error`: Postgres driver error
///
/// Output:
/// - Human-readable error string including SQLSTATE when available
///
/// Details:
/// - Adds an explicit migration hint when SQLSTATE is `42P01` (undefined table), which
///   usually indicates metastore initialization has not applied RBAC tables yet.
fn format_rbac_db_error(context: &str, error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        let sqlstate = db_error.code().code();
        if sqlstate == "42P01" {
            return format!(
                "{}: missing RBAC metastore tables (sqlstate {}). run scripts/metastore/init.sql and restart metastore",
                context, sqlstate
            );
        }

        let detail = db_error.detail().unwrap_or("");
        if detail.is_empty() {
            return format!(
                "{}: {} (sqlstate {})",
                context,
                db_error.message(),
                sqlstate
            );
        }
        return format!(
            "{}: {} (sqlstate {}, detail: {})",
            context,
            db_error.message(),
            sqlstate,
            detail
        );
    }

    format!("{}: {}", context, error)
}

/// What: Checks whether a Postgres error is a unique-constraint violation.
///
/// Inputs:
/// - `error`: Postgres driver error from an operation
///
/// Output:
/// - `true` when SQLSTATE is `23505`
///
/// Details:
/// - Used to convert database uniqueness failures into deterministic
///   user-facing RBAC validation messages.
fn is_unique_violation(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .map(|db_error| db_error.code().code() == "23505")
        .unwrap_or(false)
}

#[async_trait]
impl MetastoreProvider for PostgresProvider {
    async fn create_table(&self, req: CreateTableRequest) -> CreateTableResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateTableResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let database_name = req.database_name.trim().to_ascii_lowercase();
        let schema_name = req.schema_name.trim().to_ascii_lowercase();
        let table_name = req.table_name.trim().to_ascii_lowercase();
        if database_name.is_empty() || schema_name.is_empty() || table_name.is_empty() {
            return CreateTableResponse {
                success: false,
                message: "database_name, schema_name and table_name are required".to_string(),
            };
        }

        let qualified_schema = format!("{}.{}", database_name, schema_name);
        let get_catalog_stmt = "SELECT id::text FROM catalogs WHERE name = $1 LIMIT 1";
        let catalog_id = match client
            .query_opt(get_catalog_stmt, &[&qualified_schema])
            .await
        {
            Ok(Some(row)) => {
                let id: String = row.get(0);
                id
            }
            Ok(None) => {
                return CreateTableResponse {
                    success: false,
                    message: format!("schema not found: {}", qualified_schema),
                };
            }
            Err(e) => {
                return CreateTableResponse {
                    success: false,
                    message: format!("Failed to resolve schema catalog: {}", e),
                };
            }
        };

        let exists_stmt =
            "SELECT id::text FROM tables WHERE catalog_id::text = $1 AND name = $2 LIMIT 1";
        match client
            .query_opt(exists_stmt, &[&catalog_id, &table_name])
            .await
        {
            Ok(Some(_)) => {
                return CreateTableResponse {
                    success: false,
                    message: format!(
                        "table already exists: {}.{}.{}",
                        database_name, schema_name, table_name
                    ),
                };
            }
            Ok(None) => {}
            Err(e) => {
                return CreateTableResponse {
                    success: false,
                    message: format!("Failed to validate table existence: {}", e),
                };
            }
        }

        let columns_json = req
            .columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "name": col.name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                })
            })
            .collect::<Vec<_>>();

        let schema_json = if req.ast_payload_json.trim().is_empty() {
            serde_json::json!({
                "columns": columns_json,
            })
        } else {
            match serde_json::from_str::<serde_json::Value>(&req.ast_payload_json) {
                Ok(v) => v,
                Err(_) => serde_json::json!({
                    "columns": columns_json,
                    "ast_payload_raw": req.ast_payload_json,
                }),
            }
        };

        let insert_stmt = "INSERT INTO tables (catalog_id, name, engine, schema, location, owner) SELECT id, $2, $3, $4::text::jsonb, $5, $6 FROM catalogs WHERE name = $1 RETURNING id::text";
        let owner = "kionas";
        match client
            .query_one(
                insert_stmt,
                &[
                    &qualified_schema,
                    &table_name,
                    &req.engine,
                    &schema_json.to_string(),
                    &req.location,
                    &owner,
                ],
            )
            .await
        {
            Ok(_row) => CreateTableResponse {
                success: true,
                message: format!(
                    "Table created: {}.{}.{}",
                    database_name, schema_name, table_name
                ),
            },
            Err(e) => CreateTableResponse {
                success: false,
                message: format!("Failed to create table: {}", e),
            },
        }
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
        let database_name = req.database_name.trim().to_ascii_lowercase();
        let schema_name = req.schema_name.trim().to_ascii_lowercase();
        if database_name.is_empty() || schema_name.is_empty() {
            return CreateSchemaResponse {
                success: false,
                message: "database_name and schema_name are required".to_string(),
            };
        }

        let qualified_name = format!("{}.{}", database_name, schema_name);
        let stmt =
            "INSERT INTO catalogs (name, owner, description) VALUES ($1, $2, $3) RETURNING id";
        let owner = "kionas";
        log::debug!(
            "Creating schema '{}.{}' with owner '{}'",
            database_name,
            schema_name,
            owner
        );
        match client
            .query_one(stmt, &[&qualified_name, &owner, &req.ast_payload_json])
            .await
        {
            Ok(_row) => CreateSchemaResponse {
                success: true,
                message: format!("Schema '{}.{}' created", database_name, schema_name),
            },
            Err(e) => CreateSchemaResponse {
                success: false,
                message: format!("Failed to create schema: {}", e),
            },
        }
    }

    async fn drop_table(&self, _req: DropTableRequest) -> DropTableResponse {
        DropTableResponse {
            success: true,
            message: "Table dropped".to_string(),
        }
    }
    async fn get_table(&self, req: GetTableRequest) -> GetTableResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetTableResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    metadata: None,
                };
            }
        };

        let database_name = req.database_name.trim().to_ascii_lowercase();
        let schema_name = req.schema_name.trim().to_ascii_lowercase();
        let table_name = req.table_name.trim().to_ascii_lowercase();
        if database_name.is_empty() || schema_name.is_empty() || table_name.is_empty() {
            return GetTableResponse {
                success: false,
                message: "table not found".to_string(),
                metadata: None,
            };
        }

        let qualified_schema = format!("{}.{}", database_name, schema_name);
        let get_catalog_stmt = "SELECT id::text FROM catalogs WHERE name = $1 LIMIT 1";
        let catalog_id = match client
            .query_opt(get_catalog_stmt, &[&qualified_schema])
            .await
        {
            Ok(Some(row)) => {
                let id: String = row.get(0);
                id
            }
            Ok(None) => {
                return GetTableResponse {
                    success: false,
                    message: "table not found".to_string(),
                    metadata: None,
                };
            }
            Err(e) => {
                return GetTableResponse {
                    success: false,
                    message: format!("Failed to resolve schema catalog: {}", e),
                    metadata: None,
                };
            }
        };

        let stmt = "SELECT id::text, engine, location, schema::text FROM tables WHERE catalog_id::text = $1 AND name = $2 LIMIT 1";
        match client.query_opt(stmt, &[&catalog_id, &table_name]).await {
            Ok(Some(row)) => {
                let table_id: String = row.get(0);
                let engine: String = row.get(1);
                let location: String = row.get(2);
                let schema_raw: String = row.get(3);
                let schema_json = serde_json::from_str::<serde_json::Value>(&schema_raw)
                    .unwrap_or_else(|_| serde_json::json!({}));

                let mut columns = Vec::new();
                if let Some(raw_columns) = schema_json
                    .get("columns")
                    .and_then(serde_json::Value::as_array)
                {
                    for col in raw_columns {
                        let name = col
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let data_type = col
                            .get("data_type")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let nullable = col
                            .get("nullable")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true);
                        if !name.is_empty() {
                            columns.push(
                                crate::services::metastore_service::metastore_service::ColumnSchema {
                                    name,
                                    data_type,
                                    nullable,
                                },
                            );
                        }
                    }
                }

                GetTableResponse {
                    success: true,
                    message: format!(
                        "Table found: {}.{}.{}",
                        database_name, schema_name, table_name
                    ),
                    metadata: Some(
                        crate::services::metastore_service::metastore_service::TableMetadata {
                            uuid: table_id,
                            schema_name,
                            table_name,
                            table_type: engine,
                            location,
                            container: String::new(),
                            columns,
                            database_name,
                        },
                    ),
                }
            }
            Ok(None) => GetTableResponse {
                success: false,
                message: "table not found".to_string(),
                metadata: None,
            },
            Err(e) => GetTableResponse {
                success: false,
                message: format!("Failed to get table: {}", e),
                metadata: None,
            },
        }
    }
    async fn drop_schema(&self, _req: DropSchemaRequest) -> DropSchemaResponse {
        DropSchemaResponse {
            success: true,
            message: "Schema dropped".to_string(),
        }
    }
    async fn get_schema(&self, req: GetSchemaRequest) -> GetSchemaResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetSchemaResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    metadata: None,
                };
            }
        };

        let database_name = req.database_name.trim().to_ascii_lowercase();
        let schema_name = req.schema_name.trim().to_ascii_lowercase();
        if database_name.is_empty() || schema_name.is_empty() {
            return GetSchemaResponse {
                success: false,
                message: "schema not found".to_string(),
                metadata: None,
            };
        }

        let qualified_name = format!("{}.{}", database_name, schema_name);
        let stmt = "SELECT id::text, name FROM catalogs WHERE name = $1 LIMIT 1";
        match client.query_opt(stmt, &[&qualified_name]).await {
            Ok(Some(row)) => {
                let id: String = row.get(0);
                let name: String = row.get(1);
                GetSchemaResponse {
                    success: true,
                    message: format!("Schema '{}' found", name),
                    metadata: Some(
                        crate::services::metastore_service::metastore_service::SchemaMetadata {
                            uuid: id,
                            schema_name,
                            location: String::new(),
                            container: String::new(),
                            database_name,
                        },
                    ),
                }
            }
            Ok(None) => GetSchemaResponse {
                success: false,
                message: "schema not found".to_string(),
                metadata: None,
            },
            Err(e) => GetSchemaResponse {
                success: false,
                message: format!("Failed to get schema: {}", e),
                metadata: None,
            },
        }
    }

    async fn create_database(&self, req: CreateDatabaseRequest) -> CreateDatabaseResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateDatabaseResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let stmt = "INSERT INTO catalogs (name, owner) VALUES ($1, $2) RETURNING id";
        let owner = "kionas";
        match client.query_one(stmt, &[&req.database_name, &owner]).await {
            Ok(_row) => CreateDatabaseResponse {
                success: true,
                message: format!("Database '{}' created", req.database_name),
            },
            Err(e) => CreateDatabaseResponse {
                success: false,
                message: format!("Failed to create database: {}", e),
            },
        }
    }

    async fn get_database(&self, req: GetDatabaseRequest) -> GetDatabaseResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetDatabaseResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    metadata: None,
                };
            }
        };

        let stmt = "SELECT id::text, name FROM catalogs WHERE name = $1 LIMIT 1";
        match client.query_opt(stmt, &[&req.database_name]).await {
            Ok(Some(row)) => {
                let id: String = row.get(0);
                let name: String = row.get(1);
                GetDatabaseResponse {
                    success: true,
                    message: format!("Database '{}' found", name),
                    metadata: Some(
                        crate::services::metastore_service::metastore_service::DatabaseMetadata {
                            uuid: id,
                            database_name: name,
                            location: String::new(),
                            container: String::new(),
                        },
                    ),
                }
            }
            Ok(None) => GetDatabaseResponse {
                success: false,
                message: "database not found".to_string(),
                metadata: None,
            },
            Err(e) => GetDatabaseResponse {
                success: false,
                message: format!("Failed to get database: {}", e),
                metadata: None,
            },
        }
    }

    async fn update_table_statistics(
        &self,
        _req: UpdateTableStatisticsRequest,
    ) -> UpdateTableStatisticsResponse {
        UpdateTableStatisticsResponse {
            success: true,
            message: "Statistics updated".to_string(),
        }
    }
    async fn get_table_statistics(
        &self,
        _req: GetTableStatisticsRequest,
    ) -> GetTableStatisticsResponse {
        GetTableStatisticsResponse {
            success: true,
            message: "Statistics fetched".to_string(),
            statistics: None,
        }
    }

    async fn create_transaction(&self, req: CreateTransactionRequest) -> CreateTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateTransactionResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
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
        // Use parameterized query to avoid SQL injection and driver ToSql issues
        // Cast parameter 1 as text then to uuid so we can pass tx_id as &str
        // Cast $3 as text then to jsonb so we can pass participants as a string
        let stmt = "INSERT INTO transactions (tx_id, state, participants) VALUES ($1::text::uuid, $2, $3::text::jsonb) RETURNING tx_id::text";
        log::debug!(
            "[metastore::postgres] Executing parameterized SQL: {}",
            stmt
        );
        match client
            .query_one(stmt, &[&tx_id_str, &"PREPARING", &parts_str])
            .await
        {
            Ok(row) => {
                let returned: Result<&str, _> = row.try_get(0);
                match returned {
                    Ok(s) => match Uuid::parse_str(s) {
                        Ok(id) => log::debug!("[metastore::postgres] Insert returned tx_id={}", id),
                        Err(pe) => log::debug!(
                            "[metastore::postgres] Insert returned tx_id string but failed to parse UUID: {} (raw='{}')",
                            pe,
                            s
                        ),
                    },
                    Err(e) => log::debug!(
                        "[metastore::postgres] Insert returned a row (unable to extract tx_id): {}",
                        e
                    ),
                }
                CreateTransactionResponse {
                    success: true,
                    message: format!("Transaction {} created", req.tx_id),
                    transaction: None,
                }
            }
            Err(e) => {
                log::debug!("[metastore::postgres] Failed to create transaction: {}", e);
                CreateTransactionResponse {
                    success: false,
                    message: format!("Failed to create transaction: {}", e),
                    transaction: None,
                }
            }
        }
    }

    async fn update_transaction_state(
        &self,
        req: UpdateTransactionStateRequest,
    ) -> UpdateTransactionStateResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return UpdateTransactionStateResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
            }
        };
        let tx_id_str = &req.tx_id;
        let stmt = format!(
            "UPDATE transactions SET state = $1, updated_at = CURRENT_TIMESTAMP WHERE tx_id = '{}'::uuid RETURNING tx_id",
            tx_id_str
        );
        match client
            .query_one(&stmt, &[&format!("{:?}", req.state)])
            .await
        {
            Ok(_row) => UpdateTransactionStateResponse {
                success: true,
                message: format!("Transaction {} updated", req.tx_id),
                transaction: None,
            },
            Err(e) => UpdateTransactionStateResponse {
                success: false,
                message: format!("Failed to update transaction: {}", e),
                transaction: None,
            },
        }
    }

    async fn get_transaction(&self, req: GetTransactionRequest) -> GetTransactionResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GetTransactionResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                    transaction: None,
                };
            }
        };
        let stmt = "SELECT tx_id, state, participants, created_at, updated_at FROM transactions WHERE tx_id = $1";
        match client.query_one(stmt, &[&req.tx_id]).await {
            Ok(_row) => {
                // Optionally parse participants JSON, but return minimal response
                GetTransactionResponse {
                    success: true,
                    message: "Found".to_string(),
                    transaction: None,
                }
            }
            Err(e) => GetTransactionResponse {
                success: false,
                message: format!("Failed to get transaction: {}", e),
                transaction: None,
            },
        }
    }

    /// What: Creates a new RBAC user row in metastore.
    ///
    /// Inputs:
    /// - `req`: Create user payload
    ///
    /// Output:
    /// - `CreateUserResponse` indicating operation result
    ///
    /// Details:
    /// - Enforces reserved-name and non-empty username checks.
    async fn create_user(&self, req: CreateUserRequest) -> CreateUserResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateUserResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let username = req.username.trim().to_ascii_lowercase();
        if username.is_empty() {
            return CreateUserResponse {
                success: false,
                message: "username cannot be empty".to_string(),
            };
        }
        if username == "kionas" {
            return CreateUserResponse {
                success: false,
                message: "user 'kionas' is reserved".to_string(),
            };
        }

        let stmt = "INSERT INTO users_rbac (username, can_login) VALUES ($1, TRUE)";
        match client.execute(stmt, &[&username]).await {
            Ok(_) => CreateUserResponse {
                success: true,
                message: format!("user '{}' created", username),
            },
            Err(e) => {
                if is_unique_violation(&e) {
                    return CreateUserResponse {
                        success: false,
                        message: format!("user already exists: {}", username),
                    };
                }
                CreateUserResponse {
                    success: false,
                    message: format_rbac_db_error("failed to create user", &e),
                }
            }
        }
    }

    /// What: Deletes an RBAC user row from metastore.
    ///
    /// Inputs:
    /// - `req`: Delete user payload
    ///
    /// Output:
    /// - `DeleteUserResponse` indicating operation result
    ///
    /// Details:
    /// - Reserved bootstrap user cannot be removed.
    async fn delete_user(&self, req: DeleteUserRequest) -> DeleteUserResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return DeleteUserResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let username = req.username.trim().to_ascii_lowercase();
        if username.is_empty() {
            return DeleteUserResponse {
                success: false,
                message: "username cannot be empty".to_string(),
            };
        }
        if username == "kionas" {
            return DeleteUserResponse {
                success: false,
                message: "user 'kionas' is reserved and cannot be deleted".to_string(),
            };
        }

        let stmt = "DELETE FROM users_rbac WHERE username = $1";
        match client.execute(stmt, &[&username]).await {
            Ok(0) => DeleteUserResponse {
                success: false,
                message: format!("user not found: {}", username),
            },
            Ok(_) => DeleteUserResponse {
                success: true,
                message: format!("user '{}' deleted", username),
            },
            Err(e) => DeleteUserResponse {
                success: false,
                message: format_rbac_db_error("failed to delete user", &e),
            },
        }
    }

    /// What: Creates a new RBAC group row in metastore.
    ///
    /// Inputs:
    /// - `req`: Create group payload
    ///
    /// Output:
    /// - `CreateGroupResponse` indicating operation result
    ///
    /// Details:
    /// - Group names are normalized and validated for emptiness.
    async fn create_group(&self, req: CreateGroupRequest) -> CreateGroupResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateGroupResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let group_name = req.group_name.trim().to_ascii_lowercase();
        if group_name.is_empty() {
            return CreateGroupResponse {
                success: false,
                message: "group_name cannot be empty".to_string(),
            };
        }

        let stmt = "INSERT INTO groups_rbac (group_name) VALUES ($1)";
        match client.execute(stmt, &[&group_name]).await {
            Ok(_) => CreateGroupResponse {
                success: true,
                message: format!("group '{}' created", group_name),
            },
            Err(e) => {
                if is_unique_violation(&e) {
                    return CreateGroupResponse {
                        success: false,
                        message: format!("group already exists: {}", group_name),
                    };
                }
                CreateGroupResponse {
                    success: false,
                    message: format_rbac_db_error("failed to create group", &e),
                }
            }
        }
    }

    /// What: Deletes an RBAC group row from metastore.
    ///
    /// Inputs:
    /// - `req`: Delete group payload
    ///
    /// Output:
    /// - `DeleteGroupResponse` indicating operation result
    ///
    /// Details:
    /// - Returns not-found semantics when no row is affected.
    async fn delete_group(&self, req: DeleteGroupRequest) -> DeleteGroupResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return DeleteGroupResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let group_name = req.group_name.trim().to_ascii_lowercase();
        if group_name.is_empty() {
            return DeleteGroupResponse {
                success: false,
                message: "group_name cannot be empty".to_string(),
            };
        }

        let stmt = "DELETE FROM groups_rbac WHERE group_name = $1";
        match client.execute(stmt, &[&group_name]).await {
            Ok(0) => DeleteGroupResponse {
                success: false,
                message: format!("group not found: {}", group_name),
            },
            Ok(_) => DeleteGroupResponse {
                success: true,
                message: format!("group '{}' deleted", group_name),
            },
            Err(e) => DeleteGroupResponse {
                success: false,
                message: format_rbac_db_error("failed to delete group", &e),
            },
        }
    }

    /// What: Creates a new RBAC role row in metastore.
    ///
    /// Inputs:
    /// - `req`: Create role payload
    ///
    /// Output:
    /// - `CreateRoleResponse` indicating operation result
    ///
    /// Details:
    /// - Role name is normalized to uppercase for canonical matching.
    async fn create_role(&self, req: CreateRoleRequest) -> CreateRoleResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return CreateRoleResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let role_name = req.role_name.trim().to_ascii_uppercase();
        if role_name.is_empty() {
            return CreateRoleResponse {
                success: false,
                message: "role_name cannot be empty".to_string(),
            };
        }

        let stmt = "INSERT INTO roles_rbac (role_name, description) VALUES ($1, $2)";
        match client.execute(stmt, &[&role_name, &req.description]).await {
            Ok(_) => CreateRoleResponse {
                success: true,
                message: format!("role '{}' created", role_name),
            },
            Err(e) => {
                if is_unique_violation(&e) {
                    return CreateRoleResponse {
                        success: false,
                        message: format!("role already exists: {}", role_name),
                    };
                }
                CreateRoleResponse {
                    success: false,
                    message: format_rbac_db_error("failed to create role", &e),
                }
            }
        }
    }

    /// What: Deletes an RBAC role row from metastore.
    ///
    /// Inputs:
    /// - `req`: Drop role payload
    ///
    /// Output:
    /// - `DropRoleResponse` indicating operation result
    ///
    /// Details:
    /// - Protected ADMIN role is rejected.
    async fn drop_role(&self, req: DropRoleRequest) -> DropRoleResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return DropRoleResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let role_name = req.role_name.trim().to_ascii_uppercase();
        if role_name.is_empty() {
            return DropRoleResponse {
                success: false,
                message: "role_name cannot be empty".to_string(),
            };
        }
        if role_name == "ADMIN" {
            return DropRoleResponse {
                success: false,
                message: "role 'ADMIN' is reserved and cannot be dropped".to_string(),
            };
        }

        let stmt = "DELETE FROM roles_rbac WHERE role_name = $1";
        match client.execute(stmt, &[&role_name]).await {
            Ok(0) => DropRoleResponse {
                success: false,
                message: format!("role not found: {}", role_name),
            },
            Ok(_) => DropRoleResponse {
                success: true,
                message: format!("role '{}' dropped", role_name),
            },
            Err(e) => DropRoleResponse {
                success: false,
                message: format_rbac_db_error("failed to drop role", &e),
            },
        }
    }

    /// What: Creates a role binding for a user or group principal.
    ///
    /// Inputs:
    /// - `req`: Grant role payload
    ///
    /// Output:
    /// - `GrantRoleResponse` indicating operation result
    ///
    /// Details:
    /// - Validates principal type and resolves both principal and role IDs before insertion.
    async fn grant_role(&self, req: GrantRoleRequest) -> GrantRoleResponse {
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                return GrantRoleResponse {
                    success: false,
                    message: format!("Failed to get DB client: {}", e),
                };
            }
        };

        let principal_type = req.principal_type.trim().to_ascii_lowercase();
        if principal_type != "user" && principal_type != "group" {
            return GrantRoleResponse {
                success: false,
                message: "principal_type must be 'user' or 'group'".to_string(),
            };
        }
        let principal_name = req.principal_name.trim().to_ascii_lowercase();
        let role_name = req.role_name.trim().to_ascii_uppercase();
        if principal_name.is_empty() || role_name.is_empty() {
            return GrantRoleResponse {
                success: false,
                message: "principal_name and role_name are required".to_string(),
            };
        }

        let role_id: i64 = match client
            .query_opt(
                "SELECT id FROM roles_rbac WHERE role_name = $1",
                &[&role_name],
            )
            .await
        {
            Ok(Some(row)) => row.get(0),
            Ok(None) => {
                return GrantRoleResponse {
                    success: false,
                    message: format!("role not found: {}", role_name),
                };
            }
            Err(e) => {
                return GrantRoleResponse {
                    success: false,
                    message: format_rbac_db_error("failed to load role", &e),
                };
            }
        };

        let principal_id: i64 = if principal_type == "user" {
            match client
                .query_opt(
                    "SELECT id FROM users_rbac WHERE username = $1",
                    &[&principal_name],
                )
                .await
            {
                Ok(Some(row)) => row.get(0),
                Ok(None) => {
                    return GrantRoleResponse {
                        success: false,
                        message: format!("user not found: {}", principal_name),
                    };
                }
                Err(e) => {
                    return GrantRoleResponse {
                        success: false,
                        message: format_rbac_db_error("failed to load user principal", &e),
                    };
                }
            }
        } else {
            match client
                .query_opt(
                    "SELECT id FROM groups_rbac WHERE group_name = $1",
                    &[&principal_name],
                )
                .await
            {
                Ok(Some(row)) => row.get(0),
                Ok(None) => {
                    return GrantRoleResponse {
                        success: false,
                        message: format!("group not found: {}", principal_name),
                    };
                }
                Err(e) => {
                    return GrantRoleResponse {
                        success: false,
                        message: format_rbac_db_error("failed to load group principal", &e),
                    };
                }
            }
        };

        let granted_by = if req.granted_by.trim().is_empty() {
            "kionas".to_string()
        } else {
            req.granted_by.trim().to_ascii_lowercase()
        };

        let insert_stmt = "INSERT INTO role_bindings_rbac (principal_type, principal_id, role_id, granted_by) VALUES ($1, $2, $3, $4)";
        match client
            .execute(
                insert_stmt,
                &[&principal_type, &principal_id, &role_id, &granted_by],
            )
            .await
        {
            Ok(_) => GrantRoleResponse {
                success: true,
                message: format!(
                    "role '{}' granted to {} '{}'",
                    role_name, principal_type, principal_name
                ),
            },
            Err(e) => {
                if is_unique_violation(&e) {
                    return GrantRoleResponse {
                        success: false,
                        message: format!(
                            "role already granted: {} '{}' has role '{}'",
                            principal_type, principal_name, role_name
                        ),
                    };
                }
                GrantRoleResponse {
                    success: false,
                    message: format_rbac_db_error("failed to grant role", &e),
                }
            }
        }
    }
}
