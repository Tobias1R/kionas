use crate::core::DomainResource;
use kionas::parser::datafusion_sql::sqlparser::ast::{
    ObjectName, ColumnDef, TableConstraint, HiveDistributionStyle, HiveFormat, 
    CreateTableOptions, FileFormat, Query, CreateTableLikeKind, TableVersion, CommentDef, 
    OnCommit, Ident, Expr, OneOrManyWithParens, ClusteredBy, RowAccessPolicy, Tag, 
    StorageSerializationPolicy, RefreshModeKind, InitializeKind, WrappedCollection};
use kionas::parser::datafusion_sql::sqlparser;

#[derive(Clone, Debug)]
pub struct KionasTable {
    pub id: String,
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub dynamic: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub transient: bool,
    pub volatile: bool,
    pub iceberg: bool,
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub table_options: CreateTableOptions,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<CreateTableLikeKind>,
    pub clone: Option<ObjectName>,
    pub version: Option<TableVersion>,
    // For Hive dialect, the table comment is after the column definitions without `=`,
    // so the `comment` field is optional and different than the comment field in the general options list.
    // [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
    pub comment: Option<CommentDef>,
    pub on_commit: Option<OnCommit>,
    /// ClickHouse "ON CLUSTER" clause:
    /// <https://clickhouse.com/docs/en/sql-reference/distributed-ddl/>
    pub on_cluster: Option<Ident>,
    /// ClickHouse "PRIMARY KEY " clause.
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub primary_key: Option<Box<Expr>>,
    /// ClickHouse "ORDER BY " clause. Note that omitted ORDER BY is different
    /// than empty (represented as ()), the latter meaning "no sorting".
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub order_by: Option<OneOrManyWithParens<Expr>>,
    /// BigQuery: A partition expression for the table.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression>
    pub partition_by: Option<Box<Expr>>,
    /// BigQuery: Table clustering column list.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list>
    /// Snowflake: Table clustering list which contains base column, expressions on base columns.
    /// <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>
    pub cluster_by: Option<WrappedCollection<Vec<Expr>>>,
    /// Hive: Table clustering column list.
    /// <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>
    pub clustered_by: Option<ClusteredBy>,
    /// Postgres `INHERITs` clause, which contains the list of tables from which
    /// the new table inherits.
    /// <https://www.postgresql.org/docs/current/ddl-inherit.html>
    /// <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-PARMS-INHERITS>
    pub inherits: Option<Vec<ObjectName>>,
    /// SQLite "STRICT" clause.
    /// if the "STRICT" table-option keyword is added to the end, after the closing ")",
    /// then strict typing rules apply to that table.
    pub strict: bool,
    /// Snowflake "COPY GRANTS" clause
    
    pub copy_grants: bool,
    /// Snowflake "ENABLE_SCHEMA_EVOLUTION" clause
    
    pub enable_schema_evolution: Option<bool>,
    /// Snowflake "CHANGE_TRACKING" clause
    
    pub change_tracking: Option<bool>,
    /// Snowflake "DATA_RETENTION_TIME_IN_DAYS" clause
    
    pub data_retention_time_in_days: Option<u64>,
    /// Snowflake "MAX_DATA_EXTENSION_TIME_IN_DAYS" clause
    
    pub max_data_extension_time_in_days: Option<u64>,
    /// Snowflake "DEFAULT_DDL_COLLATION" clause
    
    pub default_ddl_collation: Option<String>,
    /// Snowflake "WITH AGGREGATION POLICY" clause
    
    pub with_aggregation_policy: Option<ObjectName>,
    /// Snowflake "WITH ROW ACCESS POLICY" clause
    
    pub with_row_access_policy: Option<RowAccessPolicy>,
    /// Snowflake "WITH TAG" clause
    
    pub with_tags: Option<Vec<Tag>>,
    /// Snowflake "EXTERNAL_VOLUME" clause for Iceberg tables
    
    pub external_volume: Option<String>,
    /// Snowflake "BASE_LOCATION" clause for Iceberg tables
    
    pub base_location: Option<String>,
    /// Snowflake "CATALOG" clause for Iceberg tables
    
    pub catalog: Option<String>,
    /// Snowflake "CATALOG_SYNC" clause for Iceberg tables
    
    pub catalog_sync: Option<String>,
    /// Snowflake "STORAGE_SERIALIZATION_POLICY" clause for Iceberg tables
    
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
    /// Snowflake "TARGET_LAG" clause for dybamic tables
    
    pub target_lag: Option<String>,
    /// Snowflake "WAREHOUSE" clause for dybamic tables
    
    pub warehouse: Option<Ident>,
    /// Snowflake "REFRESH_MODE" clause for dybamic tables
    
    pub refresh_mode: Option<RefreshModeKind>,
    /// Snowflake "INITIALIZE" clause for dybamic tables
    
    pub initialize: Option<InitializeKind>,
    /// Snowflake "REQUIRE USER" clause for dybamic tables
    
    pub require_user: bool,
}

impl KionasTable {
    pub fn new(statement: sqlparser::ast::CreateTable) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            or_replace: statement.or_replace,
            temporary: statement.temporary,
            external: statement.external,
            dynamic: statement.dynamic,
            global: statement.global,
            if_not_exists: statement.if_not_exists,
            transient: statement.transient,
            volatile: statement.volatile,
            iceberg: statement.iceberg,
            name:  statement.name,
            /// Optional schema
            columns:  statement.columns,
            constraints:  statement.constraints,
            hive_distribution:  statement.hive_distribution,
            hive_formats: statement.hive_formats,
            table_options: statement.table_options,
            file_format: statement.file_format,
            location: statement.location,
            query: statement.query,
            without_rowid: statement.without_rowid,
            like: statement.like,
            clone: statement.clone,
            version: statement.version,
            comment: statement.comment,
            on_commit: statement.on_commit,
            on_cluster: statement.on_cluster,
            primary_key: statement.primary_key,
            order_by: statement.order_by,
            partition_by: statement.partition_by,
            cluster_by: statement.cluster_by,
            clustered_by: statement.clustered_by,
            inherits: statement.inherits,
            strict: statement.strict,
            copy_grants: statement.copy_grants,
            enable_schema_evolution: statement.enable_schema_evolution,
            change_tracking: statement.change_tracking,
            data_retention_time_in_days: statement.data_retention_time_in_days,
            max_data_extension_time_in_days: statement.max_data_extension_time_in_days,
            default_ddl_collation: statement.default_ddl_collation,
            with_aggregation_policy: statement.with_aggregation_policy,
            with_row_access_policy: statement.with_row_access_policy,
            with_tags: statement.with_tags,
            external_volume: statement.external_volume,
            base_location: statement.base_location,
            catalog: statement.catalog,
            catalog_sync: statement.catalog_sync,
            storage_serialization_policy: statement.storage_serialization_policy,
            target_lag: statement.target_lag,
            warehouse: statement.warehouse,
            refresh_mode: statement.refresh_mode,
            initialize: statement.initialize,
            require_user: statement.require_user,
            
        }
    }
}

impl DomainResource for KionasTable {
    fn kind(&self) -> &'static str { "table" }

    fn validate(&self) -> Result<(), String> {
        let name = self.name.to_string();
        if name.trim().is_empty() {
            return Err("table name cannot be empty".to_string());
        }
        if self.columns.is_empty() {
            return Err("table must have at least one column".to_string());
        }
        Ok(())
    }
}
