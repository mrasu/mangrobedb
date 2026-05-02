use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::{FlightData, FlightDescriptor};
use futures::{StreamExt, TryStreamExt, stream};
use tonic::{Status, Streaming};

use crate::application::import::error::ImportError;
use crate::server::flight::server::SharedImportService;

pub async fn handle_do_put(
    import_service: &SharedImportService,
    mut stream: Streaming<FlightData>,
) -> Result<(), Status> {
    let first = stream.message().await?.ok_or_else(|| {
        Status::invalid_argument("DoPut stream must include a FlightData message")
    })?;

    let table_name = parse_import_descriptor(first.flight_descriptor.as_ref())?;
    let flight_data_stream =
        stream::once(async move { Ok(first) }).chain(stream.map_err(Into::into));
    let mut record_batches = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);

    let mut batches = Vec::new();
    while let Some(batch) = record_batches.next().await {
        let batch = batch.map_err(|error| {
            Status::invalid_argument(format!("failed to decode Arrow Flight data: {error}"))
        })?;
        batches.push(batch);
    }

    import_service
        .import(&table_name, batches)
        .map_err(import_error_to_status)?;

    Ok(())
}

fn parse_import_descriptor(descriptor: Option<&FlightDescriptor>) -> Result<String, Status> {
    let descriptor = descriptor.ok_or_else(|| {
        Status::invalid_argument("DoPut first FlightData must include descriptor")
    })?;

    match descriptor.path.as_slice() {
        [command, table_name] if command == "import" => Ok(table_name.clone()),
        _ => Err(Status::invalid_argument(
            r#"DoPut descriptor path must be ["import", table_name]"#,
        )),
    }
}

fn import_error_to_status(error: ImportError) -> Status {
    if let Some(message) = error.user_message() {
        return Status::invalid_argument(message);
    }

    eprintln!("failed to transform import RecordBatch: {error}");
    Status::internal("failed to transform import RecordBatch")
}
