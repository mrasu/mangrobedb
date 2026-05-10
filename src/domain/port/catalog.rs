use crate::domain::statistics::{ColumnStatistics, FileStatistics};
use crate::domain::table_schema::{PublicColumnDefinition, TableSchema, TableSchemaError};
use crate::infrastructure::catalog::mangrobe::{MANGROBE_DB_CATALOG_NAME, MANGROBE_DB_SCHEMA_NAME};
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
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

#[derive(Debug, Clone)]
pub struct CreateExternalTableRequest {
    pub table: ExternalTableDefinition,
    pub skip_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSummary {
    pub table_name: String,
    pub comment: Option<String>,
}

impl TableSummary {
    pub fn catalog_name(&self) -> &str {
        MANGROBE_DB_CATALOG_NAME
    }

    pub fn schema_name(&self) -> &str {
        MANGROBE_DB_SCHEMA_NAME
    }

    pub fn table_type(&self) -> String {
        "TABLE".into()
    }
}

#[derive(Debug, Clone)]
pub struct ExternalTableDefinition {
    pub table_name: String,
    pub location: ExternalLocation,
    pub format: FileFormat,
    pub columns: Vec<TableColumn>,
    pub partition_fields: Vec<PartitionField>,
    pub comment: Option<String>,
    // TODO: implement
    // pub stream_id_mapping: TableMapping,
    // pub partition_time_mapping: TableMapping,
}

impl ExternalTableDefinition {
    pub fn new(
        table_name: String,
        location: ExternalLocation,
        format: FileFormat,
        columns: Vec<TableColumn>,
        partition_fields: Vec<PartitionField>,
        comment: Option<String>,
    ) -> Self {
        Self {
            table_name,
            location,
            format,
            columns,
            partition_fields,
            comment,
        }
    }

    pub fn table_scheme(&self) -> TableSchema {
        let public_columns: Vec<_> = self
            .columns
            .iter()
            .map(|col| PublicColumnDefinition::new(col.name.clone(), col.arrow_data_type()))
            .collect();

        TableSchema::new(
            self.table_name.clone(),
            self.location.bucket.clone(),
            self.location.prefix.clone(),
            public_columns,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLocation {
    pub bucket: String,
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl ExternalLocation {
    pub fn new(
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
    ) -> Self {
        Self {
            bucket,
            prefix,
            endpoint,
            region,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumn {
    pub name: String,
    pub data_type: ColumnDataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

impl TableColumn {
    pub fn arrow_data_type(&self) -> DataType {
        match self.data_type {
            ColumnDataType::Bool => DataType::Boolean,
            ColumnDataType::Int32 => DataType::Int32,
            ColumnDataType::Int64 => DataType::Int64,
            ColumnDataType::Float64 => DataType::Float64,
            ColumnDataType::String => DataType::Utf8,
            ColumnDataType::Date => DataType::Date64,
            ColumnDataType::Time(unit) => match unit {
                TimeUnit::Second => DataType::Timestamp(Second, None),
                TimeUnit::Millisecond => DataType::Timestamp(Millisecond, None),
                TimeUnit::Microsecond => DataType::Timestamp(Microsecond, None),
                TimeUnit::Nanosecond => DataType::Timestamp(Nanosecond, None),
            },
        }
    }

    pub fn new(
        name: impl Into<String>,
        data_type: ColumnDataType,
        nullable: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            comment,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDataType {
    Bool,
    Int32,
    Int64,
    Float64,
    String,
    Date,
    Time(TimeUnit),
}

impl TryFrom<DataType> for ColumnDataType {
    type Error = TableSchemaError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        let res = match value {
            DataType::Boolean => ColumnDataType::Bool,
            DataType::Int32 => ColumnDataType::Int32,
            DataType::Int64 => ColumnDataType::Int64,
            DataType::Float64 => ColumnDataType::Float64,
            DataType::Utf8 => ColumnDataType::String,
            DataType::Date64 => ColumnDataType::Date,
            DataType::Timestamp(unit, _) => match unit {
                Second => ColumnDataType::Time(TimeUnit::Second),
                Millisecond => ColumnDataType::Time(TimeUnit::Millisecond),
                Microsecond => ColumnDataType::Time(TimeUnit::Microsecond),
                Nanosecond => ColumnDataType::Time(TimeUnit::Nanosecond),
            },
            _ => {
                return Err(TableSchemaError::UnsupportedArrowDataType { data_type: value });
            }
        };

        Ok(res)
    }
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

impl PartitionField {
    pub fn new(
        source_column: String,
        destination_column: Option<String>,
        transform: PartitionTransform,
        result_type: ColumnDataType,
    ) -> Self {
        Self {
            source_column,
            destination_column,
            transform,
            result_type,
        }
    }
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
