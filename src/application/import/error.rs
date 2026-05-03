use arrow::error::ArrowError;
use thiserror::Error;

use crate::domain::port::catalog::CatalogPortError;
use crate::domain::table_schema::TableSchemaError;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("{0}")]
    User(ImportUserError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum ImportUserError {
    #[error("failed to validate. {message}")]
    ValidationError { message: String },
    #[error("unknown import table: {table_name}")]
    InvalidTable { table_name: String },
    #[error("import request must include at least one RecordBatch")]
    EmptyImport,
    #[error("all RecordBatches in one import request must have the same schema")]
    SchemaMismatch,
    #[error("column name is reserved for mangrobe internals: {column_name}")]
    ReservedColumnName { column_name: String },
    #[error("required column is missing: {column_name}")]
    MissingColumn { column_name: String },
    #[error("incompatible type for column {column_name}: expected {expected}, got {actual}")]
    IncompatibleColumnType {
        column_name: String,
        expected: String,
        actual: String,
    },
    #[error("column must not contain null values: {column_name}")]
    NullValue { column_name: String },
    #[error("stream_id must be 0 for every row: row {row_index} has {value:?}")]
    UnsupportedStreamId {
        row_index: usize,
        value: Option<i32>,
    },
    #[error("not implemented. message: {message}")]
    NotImplemented { message: String },
}

impl ImportError {
    pub fn user_message(&self) -> Option<String> {
        match self {
            Self::User(error) => Some(error.to_string()),
            Self::Internal(_) => None,
        }
    }
}

impl From<ImportUserError> for ImportError {
    fn from(value: ImportUserError) -> Self {
        Self::User(value)
    }
}

impl From<ArrowError> for ImportError {
    fn from(value: ArrowError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<CatalogPortError> for ImportError {
    fn from(value: CatalogPortError) -> Self {
        match value {
            CatalogPortError::TableNotFound { table_name } => {
                ImportUserError::InvalidTable { table_name }.into()
            }
            CatalogPortError::Internal(error) => Self::Internal(error),
        }
    }
}

impl From<TableSchemaError> for ImportError {
    fn from(value: TableSchemaError) -> Self {
        ImportUserError::ValidationError {
            message: value.to_string(),
        }
        .into()
    }
}
