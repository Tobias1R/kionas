use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::warehouse::state::SharedData;
use deadpool::managed::{Manager, Metrics, Pool, RecycleError};

use super::identifier::normalize_identifier;
use super::relation_metadata::KionasRelationMetadata;

#[derive(Clone)]
struct KionasMetastoreManager {
    shared_data: SharedData,
}

impl Manager for KionasMetastoreManager {
    type Type = MetastoreClient;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let shared_data = self.shared_data.clone();
        async move { MetastoreClient::connect_with_shared(&shared_data).await }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> Result<(), RecycleError<Self::Error>> {
        Ok(())
    }
}

/// What: Resolve metastore relation metadata through a deadpool-managed client pool.
///
/// Inputs:
/// - Shared server state for channel/config bootstrap.
///
/// Output:
/// - Resolver capable of reading normalized table metadata for providers and handlers.
#[derive(Clone)]
pub struct KionasMetastoreResolver {
    pool: Pool<KionasMetastoreManager>,
}

impl KionasMetastoreResolver {
    /// What: Create a new deadpool-backed metastore resolver.
    ///
    /// Inputs:
    /// - `shared_data`: Shared server state.
    /// - `pool_size`: Max pooled metastore clients.
    ///
    /// Output:
    /// - Ready-to-use resolver or actionable pool build error.
    pub fn new(shared_data: SharedData, pool_size: usize) -> Result<Self, String> {
        let manager = KionasMetastoreManager { shared_data };
        let pool = Pool::builder(manager)
            .max_size(pool_size)
            .build()
            .map_err(|error| format!("metastore resolver pool build error: {}", error))?;
        Ok(Self { pool })
    }

    /// What: Load canonical relation metadata from metastore.
    ///
    /// Inputs:
    /// - `database`: Canonical database.
    /// - `schema`: Canonical schema.
    /// - `table`: Canonical table.
    ///
    /// Output:
    /// - Canonical relation metadata or actionable error string.
    pub async fn resolve_relation(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<KionasRelationMetadata, String> {
        let database = normalize_identifier(database);
        let schema = normalize_identifier(schema);
        let table = normalize_identifier(table);

        let mut client =
            self.pool.get().await.map_err(|error| {
                format!("failed to acquire metastore client from pool: {}", error)
            })?;

        let response = client
            .execute(ms::MetastoreRequest {
                action: Some(ms::metastore_request::Action::GetTable(
                    ms::GetTableRequest {
                        database_name: database.clone(),
                        schema_name: schema.clone(),
                        table_name: table.clone(),
                        location: String::new(),
                    },
                )),
            })
            .await
            .map_err(|error| {
                format!(
                    "failed to fetch metastore table metadata for {}.{}.{}: {}",
                    database, schema, table, error
                )
            })?;

        let result = response.result.ok_or_else(|| {
            format!(
                "metastore returned empty get_table result for {}.{}.{}",
                database, schema, table
            )
        })?;

        let get_table = match result {
            ms::metastore_response::Result::GetTableResponse(value) => value,
            _ => {
                return Err(format!(
                    "metastore returned unexpected get_table response for {}.{}.{}",
                    database, schema, table
                ));
            }
        };

        if !get_table.success {
            return Err(format!(
                "metastore get_table failed for {}.{}.{}: {}",
                database, schema, table, get_table.message
            ));
        }

        Ok(KionasRelationMetadata::from_metastore(
            &database,
            &schema,
            &table,
            get_table.metadata,
        ))
    }
}
