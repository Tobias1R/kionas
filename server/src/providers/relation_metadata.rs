use crate::services::metastore_client::metastore_service as ms;

use super::identifier::normalize_identifier;

/// What: Canonical column metadata resolved from metastore.
///
/// Inputs:
/// - Loaded from `ColumnSchema` entries returned by metastore.
///
/// Output:
/// - Normalized column metadata used for provider construction and validation payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KionasColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// What: Canonical relation metadata resolved from metastore.
///
/// Inputs:
/// - Namespace tuple and metastore table metadata.
///
/// Output:
/// - Deterministic relation metadata used by planner provider registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KionasRelationMetadata {
    pub database: String,
    pub schema: String,
    pub table: String,
    pub location: Option<String>,
    pub columns: Vec<KionasColumnMetadata>,
}

impl KionasRelationMetadata {
    /// What: Build canonical relation metadata from metastore `GetTable` output.
    ///
    /// Inputs:
    /// - `database`: Canonical database name.
    /// - `schema`: Canonical schema name.
    /// - `table`: Canonical table name.
    /// - `metadata`: Optional metastore table metadata payload.
    ///
    /// Output:
    /// - Normalized metadata record suitable for provider and handler consumption.
    pub fn from_metastore(
        database: &str,
        schema: &str,
        table: &str,
        metadata: Option<ms::TableMetadata>,
    ) -> Self {
        let mut columns = Vec::<KionasColumnMetadata>::new();
        let mut location = None;

        if let Some(value) = metadata {
            let loc = value.location.trim();
            if !loc.is_empty() {
                location = Some(loc.to_string());
            }

            for column in value.columns {
                let name = normalize_identifier(&column.name);
                if name.is_empty() {
                    continue;
                }
                columns.push(KionasColumnMetadata {
                    name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                });
            }
        }

        Self {
            database: normalize_identifier(database),
            schema: normalize_identifier(schema),
            table: normalize_identifier(table),
            location,
            columns,
        }
    }

    /// What: Return normalized ordered column names for lightweight handler payloads.
    ///
    /// Inputs:
    /// - Uses already normalized relation columns.
    ///
    /// Output:
    /// - Column names in metadata order.
    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }
}
