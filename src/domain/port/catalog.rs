use crate::domain::file::{
    File, FileColumnStatisticsType, FileInfo, FileMetadata, FileMetadataType,
};
use crate::domain::partition::Partition;
use crate::domain::partition_filter::PartitionFilter;
use crate::domain::statistics::FileStatistics;
use crate::domain::table_schema::TableSchema;
use crate::infrastructure::catalog::mangrobe::{MANGROBEDB_CATALOG_NAME, MANGROBEDB_SCHEMA_NAME};
use async_trait::async_trait;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait CatalogPort: Debug + Send + Sync {
    async fn create_table(&self, request: CreateTableRequest) -> Result<(), CatalogError>;

    async fn list_tables(&self) -> Result<Vec<TableSummary>, CatalogError>;

    async fn get_table(&self, table_name: &str) -> Result<TableSchema, CatalogError>;

    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError>;

    async fn get_current_state(
        &self,
        table_name: &str,
        stream: i64,
        partition_filter: &PartitionFilter,
    ) -> Result<Vec<File>, CatalogError>;

    async fn get_file_info(
        &self,
        table_name: &str,
        file_ids: &[String],
        included_column_statistics_types: &[FileColumnStatisticsType],
        included_file_metadata_types: &[FileMetadataType],
    ) -> Result<std::collections::HashMap<String, FileInfo>, CatalogError>;

    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError>;

    async fn add_files(
        &self,
        idempotency_key: &[u8],
        table_name: &str,
        stream: i64,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError>;
}

#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub table: TableSchema,
    pub skip_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSummary {
    pub table_name: String,
    pub comment: Option<String>,
}

impl TableSummary {
    pub fn catalog_name(&self) -> &str {
        MANGROBEDB_CATALOG_NAME
    }

    pub fn schema_name(&self) -> &str {
        MANGROBEDB_SCHEMA_NAME
    }

    pub fn table_type(&self) -> String {
        "TABLE".into()
    }
}

#[derive(Debug, Clone)]
pub struct AddFilesEntry {
    pub partition: Partition,
    pub files: Vec<AddFile>,
}

#[derive(Debug, Clone)]
pub struct AddFile {
    pub path: String,
    pub size: u64,
    pub column_statistics: FileStatistics,
    pub file_metadata: FileMetadata,
}
