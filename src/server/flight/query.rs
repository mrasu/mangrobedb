use crate::server::flight::error::FlightServerError;
use crate::server::flight::server::SharedQueryService;
use anyhow::anyhow;
use arrow_flight::FlightData;
use arrow_flight::utils::batches_to_flight_data;
use futures::{Stream, stream};
use std::pin::Pin;
use tonic::Status;

pub type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

pub async fn handle_do_get_statement(
    query_service: &SharedQueryService,
    sql: &str,
) -> Result<DoGetStream, FlightServerError> {
    let query_output = query_service
        .query(sql)
        .await
        .map_err(FlightServerError::from)?;

    let flight_data = batches_to_flight_data(query_output.schema.as_ref(), query_output.batches)
        .map_err(|error| FlightServerError::internal(anyhow!(error)))?;

    let output = stream::iter(flight_data.into_iter().map(Ok));
    Ok(Box::pin(output))
}
