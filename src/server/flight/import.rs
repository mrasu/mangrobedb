use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::{FlightData, FlightDescriptor};
use futures::{StreamExt, TryStreamExt, stream};
use tonic::{Status, Streaming};

pub async fn handle_do_put(mut stream: Streaming<FlightData>) -> Result<(), Status> {
    let first = stream.message().await?.ok_or_else(|| {
        Status::invalid_argument("DoPut stream must include a FlightData message")
    })?;

    let table_name = parse_import_descriptor(first.flight_descriptor.as_ref())?;
    let flight_data_stream =
        stream::once(async move { Ok(first) }).chain(stream.map_err(Into::into));
    let mut record_batches = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);

    while let Some(batch) = record_batches.next().await {
        let batch = batch.map_err(|error| {
            Status::invalid_argument(format!("failed to decode Arrow Flight data: {error}"))
        })?;
        print_record_batch(&table_name, &batch)?;
    }

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

fn print_record_batch(table_name: &str, batch: &RecordBatch) -> Result<(), Status> {
    println!("import table={table_name}");
    println!("schema={:?}", batch.schema());
    println!("rows={}", batch.num_rows());

    let formatted = pretty_format_batches(std::slice::from_ref(batch))
        .map_err(|error| Status::internal(format!("failed to format RecordBatch: {error}")))?;
    println!("{formatted}");

    Ok(())
}
