use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use thiserror::Error;
use url::ParseError;

use crate::domain::port::catalog::CatalogPortError;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("{0}")]
    User(QueryUserError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum QueryUserError {
    #[error("unknown query table: {table_name}")]
    InvalidTable { table_name: String },
}

impl QueryError {
    pub fn user_message(&self) -> Option<String> {
        match self {
            Self::User(error) => Some(error.to_string()),
            Self::Internal(_) => None,
        }
    }
}

impl From<QueryUserError> for QueryError {
    fn from(value: QueryUserError) -> Self {
        Self::User(value)
    }
}

impl From<ArrowError> for QueryError {
    fn from(value: ArrowError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<CatalogPortError> for QueryError {
    fn from(value: CatalogPortError) -> Self {
        match value {
            CatalogPortError::TableNotFound { table_name } => {
                QueryUserError::InvalidTable { table_name }.into()
            }
            CatalogPortError::Internal(error) => Self::Internal(error),
        }
    }
}

impl From<ParseError> for QueryError {
    fn from(value: ParseError) -> Self {
        anyhow::Error::new(value).into()
    }
}

impl From<DataFusionError> for QueryError {
    fn from(value: DataFusionError) -> Self {
        anyhow::Error::new(value).into()
    }
}
