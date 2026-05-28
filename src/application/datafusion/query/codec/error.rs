use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("failed to validate. {message}")]
    ValidationFailed { message: String },

    #[error("not implemented. {message}")]
    NotImplemented { message: String },

    #[error("unsupported CREATE TABLE query. {message}")]
    UnsupportedCreateTableQuery { message: String },

    #[error("invalid location. {message}")]
    InvalidLocation { message: String },
}

pub(super) fn validation_error(message: impl Into<String>) -> CodecError {
    CodecError::ValidationFailed {
        message: message.into(),
    }
}
