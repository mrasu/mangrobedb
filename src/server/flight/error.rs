use crate::application::error::ApplicationError;
use anyhow::anyhow;
use tonic::{Code, Status};
use tracing::error;

pub struct FlightServerError {
    code: Code,
    user_message: String,
    internal_error: Option<anyhow::Error>,
}

impl FlightServerError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        FlightServerError {
            code: Code::InvalidArgument,
            user_message: message.into(),
            internal_error: None,
        }
    }

    pub fn internal(message: impl Into<String>, error: anyhow::Error) -> Self {
        FlightServerError {
            code: Code::Internal,
            user_message: message.into(),
            internal_error: Some(error),
        }
    }

    pub fn from_application_error(
        default_message: impl Into<String>,
        value: ApplicationError,
    ) -> Self {
        if let Some(message) = value.user_display_message() {
            return FlightServerError {
                code: Code::InvalidArgument,
                user_message: message,
                internal_error: None,
            };
        }

        FlightServerError {
            code: Code::Internal,
            user_message: default_message.into(),
            internal_error: Some(value.into()),
        }
    }

    pub fn handle_then_to_status(self) -> Status {
        error!(code=%self.code, error = ?self.internal_error, message=self.user_message);

        Status::new(self.code, self.user_message.clone())
    }
}

impl From<Status> for FlightServerError {
    fn from(value: Status) -> Self {
        Self {
            code: value.code(),
            user_message: value.message().to_string(),
            internal_error: Some(anyhow!(value)),
        }
    }
}
