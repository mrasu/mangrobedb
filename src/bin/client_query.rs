use arrow::util::pretty::print_batches;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Channel;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";
const DEFAULT_SQL: &str = "select * from dummy_table";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    let endpoint = format!("http://{}", config.addr);
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = FlightServiceClient::new(channel);

    let ticket = Ticket {
        ticket: config.sql.into_bytes().into(),
    };

    let response = client.do_get(ticket).await?;
    let stream = response.into_inner();
    let mut record_batches = FlightRecordBatchStream::new_from_flight_data(stream.map_err(Into::into));

    let mut output = Vec::new();
    while let Some(batch) = record_batches.next().await {
        output.push(batch?);
    }

    if output.is_empty() {
        println!("query returned 0 rows");
    } else {
        print_batches(&output)?;
    }

    Ok(())
}

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: String,

    #[arg(long, default_value = DEFAULT_SQL)]
    sql: String,
}
