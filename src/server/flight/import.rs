use arrow::array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::server::PeekableFlightDataStream;
use futures::{StreamExt, TryStreamExt};

use crate::server::flight::error::FlightServerError;
use crate::server::flight::server::SharedImportService;

pub async fn do_put_statement_ingest(
    import_service: &SharedImportService,
    table_name: &str,
    stream: PeekableFlightDataStream,
) -> Result<i64, FlightServerError> {
    // TODO: receive idempotency_key for AddFiles.
    let mut record_batches =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(Into::into));

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = record_batches.next().await {
        let batch = batch.map_err(|error| {
            FlightServerError::invalid_argument(format!(
                "failed to decode Arrow Flight data: {error}"
            ))
        })?;
        batches.push(batch);
    }

    let num_rows = import_service
        .import(table_name, batches)
        .await
        .map_err(FlightServerError::from)?;

    Ok(num_rows)
}
