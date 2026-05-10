use crate::application::error::{ApplicationError, ApplicationUserError};

pub(super) fn validation_error(message: impl Into<String>) -> ApplicationError {
    ApplicationUserError::ValidationError {
        message: message.into(),
    }
    .into()
}
