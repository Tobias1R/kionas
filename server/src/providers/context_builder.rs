use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::execution::context::SessionContext;

use super::identifier::normalize_identifier;
use super::relation_metadata::KionasRelationMetadata;
use super::table_provider::KionasTableProvider;

/// What: Kionas wrapper over DataFusion catalog provider.
pub struct KionasCatalogProvider {
    pub inner: Arc<MemoryCatalogProvider>,
}

/// What: Kionas wrapper over DataFusion schema provider.
pub struct KionasSchemaProvider {
    pub inner: Arc<MemorySchemaProvider>,
}

/// What: Build a DataFusion session context with Kionas catalog/schema/table providers.
///
/// Inputs:
/// - `relations`: Canonical relation metadata resolved from metastore.
///
/// Output:
/// - Session context configured for logical and physical planning.
pub fn build_session_context_with_kionas_providers(
    relations: &[KionasRelationMetadata],
) -> Result<SessionContext, String> {
    let context = SessionContext::new();
    let mut catalogs = BTreeMap::<String, KionasCatalogProvider>::new();
    let mut schemas = BTreeMap::<(String, String), KionasSchemaProvider>::new();

    for relation in relations {
        let database = normalize_identifier(&relation.database);
        let schema = normalize_identifier(&relation.schema);
        let table = normalize_identifier(&relation.table);
        if database.is_empty() || schema.is_empty() || table.is_empty() {
            continue;
        }

        let catalog = catalogs
            .entry(database.clone())
            .or_insert_with(|| KionasCatalogProvider {
                inner: Arc::new(MemoryCatalogProvider::new()),
            });

        let schema_provider = schemas
            .entry((database.clone(), schema.clone()))
            .or_insert_with(|| KionasSchemaProvider {
                inner: Arc::new(MemorySchemaProvider::new()),
            });

        if catalog.inner.schema(schema.as_str()).is_none() {
            catalog
                .inner
                .register_schema(schema.as_str(), schema_provider.inner.clone())
                .map_err(|error| {
                    format!(
                        "failed to register schema provider {}.{}: {}",
                        database, schema, error
                    )
                })?;
        }

        let table_provider = KionasTableProvider::from_relation(relation)?;
        log::debug!(
            "kionas provider registration {}.{}.{} source={:?}",
            database,
            schema,
            table,
            table_provider.kind
        );
        schema_provider
            .inner
            .register_table(table.clone(), table_provider.provider)
            .map_err(|error| {
                format!(
                    "failed to register table provider {}.{}.{}: {}",
                    database, schema, table, error
                )
            })?;
    }

    for (catalog_name, provider) in catalogs {
        context.register_catalog(catalog_name.as_str(), provider.inner);
    }

    Ok(context)
}
