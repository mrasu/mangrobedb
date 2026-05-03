use arrow::error::ArrowError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<ArrowError> for QueryError {
    fn from(value: ArrowError) -> Self {
        anyhow::Error::new(value).into()
    }
}
