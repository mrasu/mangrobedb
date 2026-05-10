use std::fmt::Debug;

use crate::domain::statistics::{ColumnStatistics, FileStatistics};
use crate::domain::table_schema::TableSchema;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("table not found: {table_name}")]
    TableNotFound { table_name: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait CatalogPort: Debug + Send + Sync {
    async fn create_external_table(
        &self,
        request: CreateExternalTableRequest,
    ) -> Result<(), CatalogError>;

    async fn list_tables(&self) -> Result<Vec<TableSummary>, CatalogError>;

    async fn get_table(&self, table_name: &str) -> Result<ExternalTableDefinition, CatalogError>;

    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError>;

    async fn get_current_state(
        &self,
        table_name: &str,
        stream_id: i64,
        partition_time_filter: &PartitionTimeFilter,
    ) -> Result<Vec<CatalogFile>, CatalogError>;

    async fn get_file_info(
        &self,
        table_name: &str,
        file_ids: &[String],
        included_column_statistics_types: &[FileColumnStatisticsType],
        included_file_metadata_types: &[FileMetadataType],
    ) -> Result<std::collections::HashMap<String, CatalogFileInfo>, CatalogError>;

    async fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError>;

    async fn add_files(
        &self,
        idempotency_key: &[u8],
        table_name: &str,
        stream_id: i64,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateExternalTableRequest {
    pub table: ExternalTableDefinition,
    pub skip_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSummary {
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalTableDefinition {
    pub table_name: String,
    pub location: ExternalLocation,
    pub format: FileFormat,
    pub columns: Vec<TableColumn>,
    pub partition_fields: Vec<PartitionField>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLocation {
    pub bucket: String,
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumn {
    pub name: String,
    pub data_type: ColumnDataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDataType {
    Bool,
    Int64,
    Float64,
    String,
    Date,
    Time(TimeUnit),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionField {
    pub source_column: String,
    pub destination_column: Option<String>,
    pub transform: PartitionTransform,
    pub result_type: ColumnDataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTransform {
    Identity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Vortex,
}

#[derive(Debug, Clone)]
pub struct AddFilesEntry {
    pub partition_time: i64,
    pub files: Vec<AddFile>,
}

#[derive(Debug, Clone)]
pub struct AddFile {
    pub path: String,
    pub size: u64,
    pub column_statistics: FileStatistics,
    pub file_metadata: FileMetadata,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CatalogFile {
    pub file_id: String,
    pub partition_time: i64,
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CatalogFileInfo {
    pub file_id: String,
    pub path: String,
    pub size: u64,
    pub column_statistics: Vec<ColumnStatistics>,
    pub file_metadata: FileMetadata,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionTimeFilter {
    pub predicates: Vec<PartitionTimePredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTimePredicate {
    In(Vec<i64>),
    Range(PartitionTimeRange),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTimeRange {
    pub lower: Option<PartitionTimeBound>,
    pub upper: Option<PartitionTimeBound>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTimeBound {
    pub time: i64,
    pub inclusivity: BoundInclusivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundInclusivity {
    Inclusive,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileColumnStatisticsType {
    Min,
    Max,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMetadataType {
    ParquetMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct FileMetadata {
    pub parquet_metadata: Option<Vec<u8>>,
}
