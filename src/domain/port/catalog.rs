use crate::domain::statistics::FileStatistics;
use crate::domain::table_schema::TableSchema;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("table not found: {table_name}")]
    TableNotFound { table_name: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub trait CatalogPort {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, CatalogError>;

    fn get_current_state(
        &self,
        table_name: &str,
        stream_id: i32,
        partition_times: &[i64],
    ) -> Result<Vec<CatalogFile>, CatalogError>;

    fn update_table_schema(
        &self,
        table_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError>;

    fn add_files(
        &self,
        table_name: &str,
        stream_id: i32,
        entries: Vec<AddFilesEntry>,
    ) -> Result<(), CatalogError>;
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
}

#[derive(Debug, Clone)]
pub struct CatalogFile {
    pub stream_id: i32,
    pub partition_time: i64,
    pub path: String,
    pub size: u64,
    pub column_statistics: FileStatistics,
}
