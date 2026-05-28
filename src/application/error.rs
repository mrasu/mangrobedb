use crate::application::datafusion::query::codec::error::CodecError;
use crate::domain::port::catalog::CatalogError;
use crate::domain::table_schema::TableSchemaError;
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use thiserror::Error;
use url::ParseError;

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
    #[error("cannot access s3: {table_name}")]
    S3InaccessibleTable { table_name: String },
    #[error("import request must include at least one RecordBatch")]
    EmptyImport,
    #[error("all RecordBatches in one import request must have the same schema")]
    SchemaMismatch,
    #[error("column name is reserved for mangrobe internals: {column_name}")]
    ReservedColumnName { column_name: String },
    #[error("not implemented. message: {message}")]
    NotImplemented { message: String },
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

impl From<CodecError> for ApplicationError {
    fn from(value: CodecError) -> Self {
        anyhow::Error::new(value).into()
    }
}
