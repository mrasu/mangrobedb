use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::Parser;
use futures::StreamExt;
use tonic::transport::Channel;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";
const DEFAULT_CATALOG_NAME: &str = "mangrobe_db";
const DEFAULT_SCHEMA_NAME: &str = "default";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();
    let endpoint = format!("http://{}", config.addr);
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = FlightSqlServiceClient::new(channel);

    let flight_info = client
        .get_tables(CommandGetTables {
            catalog: Some(DEFAULT_CATALOG_NAME.to_string()),
            db_schema_filter_pattern: Some(DEFAULT_SCHEMA_NAME.to_string()),
            table_name_filter_pattern: None,
            table_types: Vec::new(),
            include_schema: false,
        })
        .await?;

    let endpoint = flight_info
        .endpoint
        .first()
        .ok_or_else(|| "list tables returned no flight endpoints".to_string())?;
    let ticket = endpoint
        .ticket
        .as_ref()
        .ok_or_else(|| "list tables endpoint did not include a ticket".to_string())?;
    println!("ticket: {:?}", ticket);

    let mut ret = client.do_get(ticket.clone()).await?;

    let mut query_result = Vec::<RecordBatch>::new();
    while let Some(next) = ret.next().await {
        let batch = next?;

        query_result.push(batch);
    }
    print_batches(&query_result)?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: String,
}
