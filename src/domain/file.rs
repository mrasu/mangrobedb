use crate::domain::partition::Partition;
use crate::domain::statistics::ColumnStatistics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Vortex,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct File {
    pub file_id: String,
    pub partition: Partition,
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileInfo {
    pub file_id: String,
    pub path: String,
    pub size: u64,
    pub column_statistics: Vec<ColumnStatistics>,
    pub file_metadata: FileMetadata,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMetadataType {
    ParquetMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileColumnStatisticsType {
    Min = 1,
    Max = 2,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct FileMetadata {
    pub parquet_metadata: Option<Vec<u8>>,
}
