use std::pin::Pin;

use crate::server::flight::server::SharedQueryService;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightData, Ticket};
use futures::{Stream, stream};
use tonic::Status;

pub type DoGetOutputStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

pub async fn handle_do_get(
    query_service: &SharedQueryService,
    ticket: Ticket,
) -> Result<DoGetOutputStream, Status> {
    let sql = parse_query_sql(&ticket)?;
    let query_output = query_service
        .query(&sql)
        .map_err(|error| Status::internal(format!("query failed: {error}")))?;

    let flight_data = batches_to_flight_data(query_output.schema.as_ref(), query_output.batches)
        .map_err(|error| Status::internal(format!("failed to encode FlightData: {error}")))?;
    let output = stream::iter(flight_data.into_iter().map(Ok));
    Ok(Box::pin(output))
}

pub fn parse_query_sql(ticket: &Ticket) -> Result<String, Status> {
    if ticket.ticket.is_empty() {
        return Err(Status::invalid_argument("DoGet ticket must include SQL"));
    }

    let sql = std::str::from_utf8(&ticket.ticket)
        .map_err(|error| {
            Status::invalid_argument(format!("DoGet ticket must be valid UTF-8 SQL: {error}"))
        })?
        .trim();

    if sql.is_empty() {
        return Err(Status::invalid_argument("DoGet ticket must include SQL"));
    }

    Ok(sql.to_string())
}
