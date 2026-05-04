use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use thiserror::Error;
use url::ParseError;

use crate::domain::port::catalog::CatalogError;
use crate::domain::table_schema::TableSchemaError;

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("{0}")]
    User(ApplicationUserError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum ApplicationUserError {
    #[error("failed to validate. {message}")]
    ValidationError { message: String },
    #[error("unknown table: {table_name}")]
    UnknownTable { table_name: String },
    #[error("cannot access s3: {table_name}")]
    S3InaccessibleTable { table_name: String },
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

// Error conversion policy:
// `From` is for internal errors only.
// Convert `ApplicationUserError` explicitly at each call site to decide whether it should be user-facing.
impl ApplicationError {
    pub fn user_display_message(&self) -> Option<String> {
        match self {
            Self::User(error) => Some(error.to_string()),
            Self::Internal(_) => None,
        }
    }
}

impl From<ApplicationUserError> for ApplicationError {
    fn from(value: ApplicationUserError) -> Self {
        Self::User(value)
    }
}

impl From<ArrowError> for ApplicationError {
    fn from(value: ArrowError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<CatalogError> for ApplicationError {
    fn from(value: CatalogError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<ParseError> for ApplicationError {
    fn from(value: ParseError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<DataFusionError> for ApplicationError {
    fn from(value: DataFusionError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<TableSchemaError> for ApplicationError {
    fn from(value: TableSchemaError) -> Self {
        anyhow::Error::new(value).into()
    }
}
