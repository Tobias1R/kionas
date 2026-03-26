use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};

use super::relation_metadata::KionasRelationMetadata;
use super::type_mapping::map_metastore_type_to_arrow;

/// What: Identify the source backend used to build a Kionas table provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KionasTableProviderKind {
    ListingTable,
    MemTableFallback,
}

/// What: Wrapper around DataFusion table provider with explicit source kind metadata.
pub struct KionasTableProvider {
    pub kind: KionasTableProviderKind,
    pub provider: Arc<dyn TableProvider>,
}

fn build_schema(relation: &KionasRelationMetadata) -> Arc<Schema> {
    let mut fields = Vec::<Field>::new();
    for column in &relation.columns {
        if column.name.is_empty() {
            continue;
        }
        fields.push(Field::new(
            column.name.clone(),
            map_metastore_type_to_arrow(&column.data_type),
            column.nullable,
        ));
    }

    if fields.is_empty() {
        fields.push(Field::new(
            "_kionas_placeholder",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        ));
    }

    Arc::new(Schema::new(fields))
}

fn try_listing_provider(
    relation: &KionasRelationMetadata,
    schema: Arc<Schema>,
) -> Result<Arc<dyn TableProvider>, String> {
    let location = relation
        .location
        .as_ref()
        .ok_or_else(|| "relation location missing for ListingTable provider".to_string())?;
    let table_url = ListingTableUrl::parse(location)
        .map_err(|error| format!("invalid ListingTable URL '{}': {}", location, error))?;

    let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)
        .map_err(|error| format!("failed to build ListingTable provider: {}", error))?;
    Ok(Arc::new(table))
}

fn build_mem_fallback(schema: Arc<Schema>) -> Result<Arc<dyn TableProvider>, String> {
    let table = MemTable::try_new(schema, vec![vec![]])
        .map_err(|error| format!("failed to build MemTable fallback provider: {}", error))?;
    Ok(Arc::new(table))
}

impl KionasTableProvider {
    /// What: Build a Kionas table provider from canonical relation metadata.
    ///
    /// Inputs:
    /// - `relation`: Canonical relation metadata.
    ///
    /// Output:
    /// - ListingTable provider when possible, otherwise MemTable fallback.
    ///
    /// Details:
    /// - This phase prefers ListingTable for production relation scans and keeps
    ///   MemTable fallback for test/degraded planning paths.
    pub fn from_relation(relation: &KionasRelationMetadata) -> Result<Self, String> {
        let schema = build_schema(relation);

        match try_listing_provider(relation, schema.clone()) {
            Ok(provider) => Ok(Self {
                kind: KionasTableProviderKind::ListingTable,
                provider,
            }),
            Err(error) => {
                log::warn!(
                    "kionas provider falling back to MemTable for {}.{}.{}: {}",
                    relation.database,
                    relation.schema,
                    relation.table,
                    error
                );
                let provider = build_mem_fallback(schema)?;
                Ok(Self {
                    kind: KionasTableProviderKind::MemTableFallback,
                    provider,
                })
            }
        }
    }
}
