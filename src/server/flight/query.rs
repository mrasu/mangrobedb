use crate::application::error::ApplicationError;
use crate::server::flight::error::FlightServerError;
use crate::server::flight::server::SharedQueryService;
use anyhow::anyhow;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightData, Ticket};
use futures::{Stream, stream};
use std::pin::Pin;
use tonic::Status;

pub type DoGetOutputStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

pub async fn handle_do_get(
    query_service: &SharedQueryService,
    ticket: Ticket,
) -> Result<DoGetOutputStream, FlightServerError> {
    let sql = parse_query_sql(&ticket)?;
    let query_output = query_service
        .query(&sql)
        .await
        .map_err(query_error_to_status)?;

    let flight_data = batches_to_flight_data(query_output.schema.as_ref(), query_output.batches)
        .map_err(|error| {
            FlightServerError::internal("failed to encode FlightData", anyhow!(error))
        })?;
    let output = stream::iter(flight_data.into_iter().map(Ok));
    Ok(Box::pin(output))
}

fn query_error_to_status(error: ApplicationError) -> FlightServerError {
    FlightServerError::from_application_error("query failed", error)
}

pub fn parse_query_sql(ticket: &Ticket) -> Result<String, FlightServerError> {
    if ticket.ticket.is_empty() {
        return Err(FlightServerError::invalid_argument(
            "DoGet ticket must include SQL",
        ));
    }

    let sql = std::str::from_utf8(&ticket.ticket)
        .map_err(|error| {
            FlightServerError::invalid_argument(format!(
                "DoGet ticket must be valid UTF-8 SQL: {error}"
            ))
        })?
        .trim();

    if sql.is_empty() {
        return Err(FlightServerError::invalid_argument(
            "DoGet ticket must include SQL",
        ));
    }

    Ok(sql.to_string())
}
