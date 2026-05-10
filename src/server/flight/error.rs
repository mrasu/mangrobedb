use crate::application::error::ApplicationError;
use anyhow::anyhow;
use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use tonic::{Code, Status};
use tracing::error;

pub struct FlightServerError {
    code: Code,
    error: Option<anyhow::Error>,
    flight_status: Option<Status>,
}

impl FlightServerError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        FlightServerError {
            code: Code::InvalidArgument,
            error: Some(anyhow!(message.into())),
            flight_status: None,
        }
    }

    pub fn unimplemented(message: impl Into<String>) -> Self {
        FlightServerError {
            code: Code::Unimplemented,
            error: Some(anyhow!(message.into())),
            flight_status: None,
        }
    }

    pub fn internal(error: anyhow::Error) -> Self {
        FlightServerError {
            code: Code::InvalidArgument,
            error: Some(error),
            flight_status: None,
        }
    }

    pub fn handle_then_to_status(self) -> Status {
        if let Some(error) = self.error {
            error!(code=%self.code, error = ?error);

            return Status::new(self.code, error.to_string());
        }

        if let Some(status) = self.flight_status {
            error!(code=%self.code, ?status);
            return status;
        };

        Status::new(self.code, "unknown")
    }
}

impl From<Status> for FlightServerError {
    fn from(value: Status) -> Self {
        Self {
            code: value.code(),
            error: None,
            flight_status: Some(value),
        }
    }
}

impl From<ApplicationError> for FlightServerError {
    fn from(value: ApplicationError) -> Self {
        match value {
            ApplicationError::User(error) => Self::invalid_argument(error.to_string()),
            ApplicationError::Internal(error) => Self::internal(error),
        }
    }
}

impl From<anyhow::Error> for FlightServerError {
    fn from(value: anyhow::Error) -> Self {
        Self::internal(value)
    }
}

impl From<ArrowError> for FlightServerError {
    fn from(value: ArrowError) -> Self {
        Self::internal(anyhow!(value))
    }
}

impl From<FlightError> for FlightServerError {
    fn from(value: FlightError) -> Self {
        Status::from(value).into()
    }
}
